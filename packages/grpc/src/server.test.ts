import { create, toBinary, type JsonValue, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import {
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  FileDescriptorProtoSchema,
  ValueSchema,
} from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import {
  viewServerDecodeSourceHealth,
  viewServerEncodeSourceHealth,
} from "@effect-view-server/protocol";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import {
  Effect,
  Exit,
  Fiber,
  Context,
  Layer,
  Logger,
  Option,
  References,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import type {
  SourceDegradationReasons,
  SourceItemRejectionDiagnostic,
} from "effect-view-server/source-adapter";
import { grpc, GrpcSourceAdapter, type GrpcMaterializedDefinitionOptions } from "./model";
import { grpcServerLayer, grpcServiceBindings, type GrpcRuntimeClient } from "./server";
import {
  grpcBindingPlan,
  grpcServerLayerFromBindingPlan,
  makeOwnedGrpcIterable,
} from "./server-internal";
import { awaitTestCondition } from "./test-support";

type RequestMessage = Message<"grpc.server.Request"> & {
  readonly metadata: JsonValue | undefined;
  readonly region: string;
  readonly tags: Array<string>;
};

type EventMessage = Message<"grpc.server.Event"> & {
  readonly id: string;
  readonly price: number;
  readonly region: string;
};

const latestSourceItemRejection = <AdapterFailure, RejectionLocation>(
  reasons: SourceDegradationReasons<AdapterFailure, RejectionLocation>,
): SourceItemRejectionDiagnostic<AdapterFailure, RejectionLocation> | undefined => {
  const first = reasons[0];
  return first._tag === "SourceItemRejection" ? first.latestRejection : undefined;
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/server.proto",
          package: "grpc.server",
          syntax: "proto3",
          dependency: ["google/protobuf/struct.proto"],
          messageType: [
            {
              name: "Request",
              field: [
                {
                  name: "region",
                  number: 1,
                  type: FieldDescriptorProto_Type.STRING,
                },
                {
                  name: "metadata",
                  number: 2,
                  type: FieldDescriptorProto_Type.MESSAGE,
                  typeName: ".google.protobuf.Value",
                },
                {
                  name: "tags",
                  number: 3,
                  label: FieldDescriptorProto_Label.REPEATED,
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
                  name: "price",
                  number: 2,
                  type: FieldDescriptorProto_Type.DOUBLE,
                },
                {
                  name: "region",
                  number: 3,
                  type: FieldDescriptorProto_Type.STRING,
                },
              ],
            },
          ],
          service: [
            {
              name: "Orders",
              method: [
                {
                  name: "Stream",
                  inputType: ".grpc.server.Request",
                  outputType: ".grpc.server.Event",
                  serverStreaming: true,
                },
              ],
            },
            {
              name: "Inventory",
              method: [
                {
                  name: "Stream",
                  inputType: ".grpc.server.Request",
                  outputType: ".grpc.server.Event",
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
  [ValueSchema.file],
);

const RequestSchema = messageDesc<RequestMessage>(descriptorFile, 0);
const EventSchema = messageDesc<EventMessage>(descriptorFile, 1);
const OrdersService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 0);
const InventoryService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 1);

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
  region: Schema.String,
});

type StreamCommand =
  | {
      readonly _tag: "Value";
      readonly value: unknown;
    }
  | {
      readonly _tag: "Fail";
      readonly cause?: unknown;
    }
  | {
      readonly _tag: "Complete";
    };

type Pending = {
  readonly resolve: (result: IteratorResult<unknown>) => void;
  readonly reject: (error: unknown) => void;
};

type Invocation = {
  readonly method: string;
  readonly request: unknown;
  readonly signal: AbortSignal;
  readonly commands: Array<StreamCommand>;
  readonly pending: Array<Pending>;
  returns: number;
};

const applyCommand = (invocation: Invocation, command: StreamCommand): void => {
  const pending = invocation.pending.shift();
  if (pending === undefined) {
    invocation.commands.push(command);
    return;
  }
  if (command._tag === "Value") {
    pending.resolve({ done: false, value: command.value });
  } else if (command._tag === "Complete") {
    pending.resolve({ done: true, value: undefined });
  } else {
    pending.reject(command.cause ?? new Error("planned upstream stream failure"));
  }
};

const takeCommand = (invocation: Invocation): Promise<IteratorResult<unknown>> => {
  const command = invocation.commands.shift();
  if (command !== undefined) {
    if (command._tag === "Value") {
      return Promise.resolve({ done: false, value: command.value });
    }
    if (command._tag === "Complete") {
      return Promise.resolve({ done: true, value: undefined });
    }
    return Promise.reject(command.cause ?? new Error("planned upstream stream failure"));
  }
  return new Promise((resolve, reject) => {
    invocation.pending.push({ resolve, reject });
  });
};

const makeControlledClient = (input?: {
  readonly rejectFinalization?: boolean;
  readonly invocationFailure?: boolean;
  readonly invocationFailureCause?: unknown;
  readonly invalidIterable?: boolean;
  readonly throwingIterableAccessor?: boolean;
  readonly iteratorFailure?: boolean;
  readonly invalidIterator?: boolean;
  readonly throwingIteratorAccessor?: boolean;
  readonly omitIteratorReturn?: boolean;
  readonly invalidIteratorReturn?: boolean;
}) => {
  const invocations: Array<Invocation> = [];
  const client: GrpcRuntimeClient = {
    service: OrdersService,
    invoke: (method, request, signal) => {
      if (input?.invocationFailureCause !== undefined) {
        throw input.invocationFailureCause;
      }
      if (input?.invocationFailure === true) {
        throw new Error("planned invocation failure");
      }
      if (input?.invalidIterable === true) {
        return {};
      }
      if (input?.throwingIterableAccessor === true) {
        return Object.defineProperty({}, Symbol.asyncIterator, {
          get: () => {
            throw new Error("planned iterable accessor failure");
          },
        });
      }
      const invocation: Invocation = {
        method,
        request,
        signal,
        commands: [],
        pending: [],
        returns: 0,
      };
      invocations.push(invocation);
      return {
        [Symbol.asyncIterator]: () => {
          if (input?.iteratorFailure === true) {
            throw new Error("planned iterator acquisition failure");
          }
          if (input?.invalidIterator === true) {
            return {};
          }
          if (input?.throwingIteratorAccessor === true) {
            return Object.defineProperty({}, "next", {
              get: () => {
                throw new Error("planned iterator accessor failure");
              },
            });
          }
          const iterator = {
            next: () => takeCommand(invocation),
            return: () => {
              invocation.returns += 1;
              const pending = invocation.pending.splice(0);
              for (const waiter of pending) {
                waiter.resolve({ done: true, value: undefined });
              }
              if (input?.rejectFinalization === true) {
                return Promise.reject(new Error("planned finalization failure"));
              }
              if (input?.invalidIteratorReturn === true) {
                return Promise.resolve("invalid iterator result");
              }
              return Promise.resolve({ done: true, value: undefined });
            },
          };
          if (input?.omitIteratorReturn === true) {
            return {
              next: iterator.next,
            };
          }
          return iterator;
        },
      };
    },
  };
  return {
    client,
    invocations,
    emit: (index: number, value: unknown) =>
      Effect.sync(() => {
        const invocation = invocations[index];
        if (invocation === undefined) {
          throw new TypeError(`Missing controlled invocation ${index}.`);
        }
        applyCommand(invocation, { _tag: "Value", value });
      }),
    fail: (index: number, cause?: unknown) =>
      Effect.sync(() => {
        const invocation = invocations[index];
        if (invocation === undefined) {
          throw new TypeError(`Missing controlled invocation ${index}.`);
        }
        applyCommand(invocation, { _tag: "Fail", ...(cause === undefined ? {} : { cause }) });
      }),
    complete: (index: number) =>
      Effect.sync(() => {
        const invocation = invocations[index];
        if (invocation === undefined) {
          throw new TypeError(`Missing controlled invocation ${index}.`);
        }
        applyCommand(invocation, { _tag: "Complete" });
      }),
  };
};

const awaitCondition = (
  label: () => string,
  predicate: () => boolean,
  remaining = 10_000,
): Effect.Effect<void> => awaitTestCondition(label, predicate, remaining, Effect.yieldNow);

const awaitInvocationCount = (controlled: ReturnType<typeof makeControlledClient>, count: number) =>
  awaitCondition(
    () => `controlled invocation count ${count}; last observed ${controlled.invocations.length}`,
    () => controlled.invocations.length === count,
  );

const requestField = (request: unknown, field: string): unknown =>
  Reflect.get(Object(request), field);

const dataEvents = <A, E, R>(events: Stream.Stream<A, E, R>) =>
  events.pipe(
    Stream.filter(
      (event): event is Extract<A, { readonly type: "snapshot" | "delta" }> =>
        typeof event === "object" &&
        event !== null &&
        (Reflect.get(event, "type") === "snapshot" || Reflect.get(event, "type") === "delta"),
    ),
  );

const isClosedLayer = (value: unknown): value is Layer.Layer<unknown, unknown, never> =>
  Layer.isLayer(value);

const rawMaterializedSource = (options: unknown): unknown =>
  Reflect.apply(GrpcSourceAdapter.materializedSource, GrpcSourceAdapter, [options]);

describe("gRPC Source Adapter Runtime Core vertical slice", () => {
  it.effect("short-circuits owned iterator reads after finalization starts", () =>
    Effect.gen(function* () {
      let nextCalls = 0;
      let returnCalls = 0;
      const controller = new AbortController();
      const owned = makeOwnedGrpcIterable(
        {
          next: () => {
            nextCalls += 1;
            return Promise.resolve({
              done: false,
              value: "unexpected",
            });
          },
          return: () => {
            returnCalls += 1;
            return Promise.resolve({
              done: true,
              value: undefined,
            });
          },
        },
        controller,
      );
      const iterator = owned.iterable[Symbol.asyncIterator]();
      const returnMethod = Reflect.get(iterator, "return");
      if (typeof returnMethod !== "function") {
        return yield* Effect.die("Expected owned iterator return method.");
      }
      const returned = yield* Effect.promise(() => Reflect.apply(returnMethod, iterator, []));
      const afterFinalization = yield* Effect.promise(() => iterator.next());

      expect({
        returned,
        afterFinalization,
        nextCalls,
        returnCalls,
        aborted: controller.signal.aborted,
      }).toStrictEqual({
        returned: {
          done: true,
          value: undefined,
        },
        afterFinalization: {
          done: true,
          value: undefined,
        },
        nextCalls: 0,
        returnCalls: 1,
        aborted: true,
      });
    }),
  );

  it.effect("starts Materialized sources, reuses frozen requests across retry, and cancels", () =>
    Effect.gen(function* () {
      const controlled = makeControlledClient();
      let requestCalls = 0;
      const source = grpc.topicSources({ orders: OrdersService }).materialized(
        {
          client: "orders",
          method: "stream",
          request: () => {
            requestCalls += 1;
            return {
              metadata: ["stable", { identity: "browser-identity-sentinel" }],
              region: "all",
            };
          },
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        },
        Schedule.recurs(1),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
      );
      yield* awaitInvocationCount(controlled, 1);
      const firstRequest = controlled.invocations[0]?.request;
      const metadata = requestField(firstRequest, "metadata");
      const metadataKind = requestField(metadata, "kind");
      const metadataList = requestField(metadataKind, "value");
      const metadataValues = requestField(metadataList, "values");
      const metadataMutationAccepted = Reflect.set(Object(metadataKind), "case", "nullValue");
      const defaultTagsMutationAccepted = Reflect.set(
        Object(requestField(firstRequest, "tags")),
        0,
        "mutated",
      );
      yield* controlled.fail(0);
      yield* awaitInvocationCount(controlled, 2);
      const subscription = yield* runtime.liveClient.subscribe("orders", {
        select: ["id", "price", "region"],
      });
      const eventsFiber = yield* dataEvents(subscription.events).pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* controlled.emit(1, { id: "one", price: 10, region: "eu" });
      const events = yield* Fiber.join(eventsFiber);
      yield* TestClock.adjust("1 second");
      const metricsDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      const sampled = Option.getOrThrow(
        yield* metricsDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );

      expect({
        requestCalls,
        frozen: Object.isFrozen(firstRequest),
        metadataFrozen: Object.isFrozen(metadata),
        metadataKindFrozen: Object.isFrozen(metadataKind),
        metadataListFrozen: Object.isFrozen(metadataList),
        metadataValuesFrozen: Object.isFrozen(metadataValues),
        metadataMutationAccepted,
        defaultTagsFrozen: Object.isFrozen(requestField(firstRequest, "tags")),
        defaultTagsMutationAccepted,
        metadataCaseAfterRetry: requestField(metadataKind, "case"),
        metadataFirstValueAfterRetry: requestField(
          requestField(
            requestField(
              requestField(
                requestField(Array.isArray(metadataValues) ? metadataValues[1] : undefined, "kind"),
                "value",
              ),
              "fields",
            ),
            "identity",
          ),
          "kind",
        ),
        sameRequest: firstRequest === controlled.invocations[1]?.request,
        method: controlled.invocations[1]?.method,
        eventTypes: events.map((event) => event.type),
        adapterMetrics: sampled.metrics.adapter,
      }).toStrictEqual({
        requestCalls: 1,
        frozen: true,
        metadataFrozen: true,
        metadataKindFrozen: true,
        metadataListFrozen: true,
        metadataValuesFrozen: true,
        metadataMutationAccepted: false,
        defaultTagsFrozen: true,
        defaultTagsMutationAccepted: false,
        metadataCaseAfterRetry: "listValue",
        metadataFirstValueAfterRetry: {
          case: "stringValue",
          value: "browser-identity-sentinel",
        },
        sameRequest: true,
        method: "stream",
        eventTypes: ["snapshot", "delta"],
        adapterMetrics: {
          logicalClient: "orders",
          method: "stream",
          activeInvocations: 1n,
          acquisitionCount: 2n,
          reconnectCount: 1n,
          completionCount: 0n,
          streamFailureCount: 1n,
          messageCount: 1n,
          mappedMessageCount: 1n,
          rejectedMessageCount: 0n,
          cancellationCount: 0n,
          finalizationCount: 1n,
          finalizationFailureCount: 0n,
        },
      });
      const encodedSampled = yield* viewServerEncodeSourceHealth(config, "orders", sampled);
      expect(yield* viewServerDecodeSourceHealth(config, "orders", encodedSampled)).toStrictEqual(
        sampled,
      );

      yield* subscription.close();
      yield* metricsDiagnostics.close();
      yield* runtime.close;
      yield* awaitCondition(
        () =>
          `second controlled invocation return count 1; last observed ${
            controlled.invocations[1]?.returns ?? 0
          }`,
        () => controlled.invocations[1]?.returns === 1,
      );
      expect({
        aborted: controlled.invocations[1]?.signal.aborted,
        returns: controlled.invocations[1]?.returns,
      }).toStrictEqual({
        aborted: true,
        returns: 1,
      });
    }),
  );

  it.effect(
    "shares exact Leased routes, isolates distinct routes, and reevaluates after release",
    () =>
      Effect.gen(function* () {
        const controlled = makeControlledClient();
        let requestCalls = 0;
        const source = grpc.topicSources({ orders: OrdersService }).leased({
          client: "orders",
          method: "stream",
          routeBy: ["region"],
          request: (route) => {
            requestCalls += 1;
            return { region: route.region };
          },
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        });
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source,
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({
          topic: "orders",
          routeBy: { region: "eu" },
        });
        const inactive = Option.getOrThrow(
          yield* diagnostics.events.pipe(Stream.take(1), Stream.runHead),
        );
        const encodedInactive = yield* viewServerEncodeSourceHealth(config, "orders", inactive);
        expect(
          yield* viewServerDecodeSourceHealth(config, "orders", encodedInactive),
        ).toStrictEqual(inactive);
        yield* Effect.yieldNow;
        expect(controlled.invocations.length).toBe(0);

        const firstEu = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          select: ["id", "region"],
        });
        const secondEu = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          where: [{ field: "price", type: "greaterThan", filter: 1 }],
          select: ["id", "price", "region"],
          orderBy: [{ field: "price", direction: "desc" }],
        });
        const groupedEu = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          groupBy: ["region"],
          aggregates: {
            rowCount: {
              aggFunc: "count",
            },
          },
          limit: 1,
        });
        const paginatedEu = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          select: ["id", "price", "region"],
          orderBy: [{ field: "price", direction: "asc" }],
          offset: 1,
          limit: 1,
        });
        yield* awaitInvocationCount(controlled, 1);
        const us = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "us" },
          select: ["id", "region"],
        });
        yield* awaitInvocationCount(controlled, 2);

        expect({
          requestCalls,
          euRegion: requestField(controlled.invocations[0]?.request, "region"),
          usRegion: requestField(controlled.invocations[1]?.request, "region"),
        }).toStrictEqual({
          requestCalls: 2,
          euRegion: "eu",
          usRegion: "us",
        });

        yield* firstEu.close();
        expect(controlled.invocations[0]?.returns).toBe(0);
        yield* secondEu.close();
        expect(controlled.invocations[0]?.returns).toBe(0);
        yield* groupedEu.close();
        expect(controlled.invocations[0]?.returns).toBe(0);
        yield* paginatedEu.close();
        yield* awaitCondition(
          () =>
            `first controlled invocation return count 1; last observed ${
              controlled.invocations[0]?.returns ?? 0
            }`,
          () => controlled.invocations[0]?.returns === 1,
        );
        const reacquiredEu = yield* runtime.liveClient.subscribe("orders", {
          routeBy: { region: "eu" },
          select: ["id"],
        });
        yield* awaitInvocationCount(controlled, 3);
        expect({
          requestCalls,
          oldRequestIsNew:
            controlled.invocations[0]?.request === controlled.invocations[2]?.request,
        }).toStrictEqual({
          requestCalls: 3,
          oldRequestIsNew: false,
        });

        yield* reacquiredEu.close();
        yield* us.close();
        yield* diagnostics.close();
        yield* runtime.close;
        yield* awaitCondition(
          () =>
            `remaining controlled invocation return counts 1 and 1; last observed ${
              controlled.invocations[1]?.returns ?? 0
            } and ${controlled.invocations[2]?.returns ?? 0}`,
          () =>
            controlled.invocations[1]?.returns === 1 && controlled.invocations[2]?.returns === 1,
        );
      }),
  );

  it.effect("records Mapping, row, and route rejections while later values continue", () =>
    Effect.gen(function* () {
      const controlled = makeControlledClient();
      const source = grpc.topicSources({ orders: OrdersService }).leased({
        client: "orders",
        method: "stream",
        routeBy: ["region"],
        request: (route) => ({ region: route.region }),
        map: ({ value }) => {
          if (value.id === "throw") {
            throw new Error("planned Mapping failure");
          }
          return {
            id: value.id,
            price: value.price,
            region: value.region,
          };
        },
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
      );
      const subscription = yield* runtime.liveClient.subscribe("orders", {
        routeBy: { region: "eu" },
        select: ["id", "price", "region"],
        orderBy: [{ field: "id", direction: "asc" }],
      });
      const leaseKeeper = yield* runtime.liveClient.subscribe("orders", {
        routeBy: { region: "eu" },
        select: ["id"],
      });
      const dataFiber = yield* dataEvents(subscription.events).pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
        routeBy: { region: "eu" },
      });
      const rejectionDiagnostics = yield* Effect.forEach(
        [1n, 2n, 3n, 4n] as const,
        (streamItemIndex) =>
          runtime.liveClient
            .subscribeSourceHealth({ topic: "orders", routeBy: { region: "eu" } })
            .pipe(Effect.map((events) => ({ events, streamItemIndex }))),
      );
      const rejectionFibers = yield* Effect.forEach(rejectionDiagnostics, (entry) =>
        entry.events.events.pipe(
          Stream.filter(
            (health) =>
              health._tag === "Active" &&
              health.health.status._tag === "Degraded" &&
              health.health.status.reasons.some(
                (reason) =>
                  reason._tag === "SourceItemRejection" &&
                  reason.latestRejection.location.streamItemIndex === entry.streamItemIndex,
              ),
          ),
          Stream.take(1),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
          Effect.forkChild,
        ),
      );
      yield* Effect.yieldNow;
      yield* awaitInvocationCount(controlled, 1);
      yield* controlled.emit(0, { id: "throw", price: 1, region: "eu" });
      const firstDegraded = yield* Fiber.join(
        Option.getOrThrow(Option.fromUndefinedOr(rejectionFibers[0])),
      );
      yield* controlled.emit(0, { id: "wrong-row", price: "invalid", region: "eu" });
      const secondDegraded = yield* Fiber.join(
        Option.getOrThrow(Option.fromUndefinedOr(rejectionFibers[1])),
      );
      yield* controlled.emit(0, { id: "", price: 2, region: "eu" });
      const thirdDegraded = yield* Fiber.join(
        Option.getOrThrow(Option.fromUndefinedOr(rejectionFibers[2])),
      );
      yield* controlled.emit(0, { id: "wrong-route", price: 2, region: "us" });
      const fourthDegraded = yield* Fiber.join(
        Option.getOrThrow(Option.fromUndefinedOr(rejectionFibers[3])),
      );
      yield* controlled.emit(0, { id: "valid", price: 3, region: "eu" });
      const events = yield* Fiber.join(dataFiber);
      const degraded = fourthDegraded;
      if (degraded._tag !== "Active") {
        return yield* Effect.die("Expected active Leased diagnostics.");
      }
      if (degraded.health.status._tag !== "Degraded") {
        return yield* Effect.die("Expected Degraded Leased diagnostics.");
      }
      yield* TestClock.adjust("1 second");
      const sampledDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
        routeBy: { region: "eu" },
      });
      const sampledActive = Option.getOrThrow(
        yield* sampledDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      if (sampledActive._tag !== "Active") {
        return yield* Effect.die("Expected sampled active Leased diagnostics.");
      }
      const aggregateHealth = yield* runtime.refreshHealth;
      const availabilitySubscription = yield* runtime.liveClient.subscribe("orders", {
        routeBy: { region: "eu" },
        select: ["id"],
      });
      const availability = Option.getOrThrow(
        yield* availabilitySubscription.events.pipe(
          Stream.filter((event) => event.type === "status"),
          Stream.take(1),
          Stream.runHead,
        ),
      );

      expect({
        aggregateStatus: aggregateHealth.status,
        aggregateTopicStatus: aggregateHealth.engine.topics.orders.status,
        availability: {
          code: availability.code,
          status: availability.status,
        },
        eventTypes: events.map((event) => event.type),
        status: degraded.health.status._tag,
        rejectedItemCount: degraded.health.metrics.runtime.rejectedItemCount,
        latestLocation: latestSourceItemRejection(degraded.health.status.reasons)?.location,
        rejectionLocations: [firstDegraded, secondDegraded, thirdDegraded, fourthDegraded].map(
          (health) =>
            health._tag === "Active" && health.health.status._tag === "Degraded"
              ? latestSourceItemRejection(health.health.status.reasons)?.location
              : undefined,
        ),
        rejectionFailures: [firstDegraded, secondDegraded, thirdDegraded, fourthDegraded].map(
          (health) =>
            health._tag === "Active" && health.health.status._tag === "Degraded"
              ? latestSourceItemRejection(health.health.status.reasons)?.failure
              : undefined,
        ),
        adapterMetrics: sampledActive.health.metrics.adapter,
      }).toStrictEqual({
        aggregateStatus: "degraded",
        aggregateTopicStatus: "degraded",
        availability: {
          code: "Ready",
          status: "ready",
        },
        eventTypes: ["snapshot", "delta"],
        status: "Degraded",
        rejectedItemCount: 4n,
        latestLocation: {
          client: "orders",
          method: "stream",
          phase: "route",
          streamItemIndex: 4n,
        },
        rejectionLocations: [
          {
            client: "orders",
            method: "stream",
            phase: "mapping",
            streamItemIndex: 1n,
          },
          {
            client: "orders",
            method: "stream",
            phase: "row",
            streamItemIndex: 2n,
          },
          {
            client: "orders",
            method: "stream",
            phase: "row",
            streamItemIndex: 3n,
          },
          {
            client: "orders",
            method: "stream",
            phase: "route",
            streamItemIndex: 4n,
          },
        ],
        rejectionFailures: [
          {
            _tag: "AdapterFailure",
            failure: {
              _tag: "GrpcMappingFailure",
              client: "orders",
              method: "stream",
              message: "The synchronous gRPC Mapping callback threw.",
            },
          },
          {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidTopicRow",
              topic: "orders",
              message: "Source Upsert does not satisfy Topic orders Schema.",
            },
          },
          {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidCanonicalId",
              topic: "orders",
              message: "Source Topic orders requires a non-empty canonical string id.",
            },
          },
          {
            _tag: "RuntimeFailure",
            failure: {
              _tag: "InvalidFeedRoute",
              topic: "orders",
              message: "Source Topic orders row does not match the acquired Feed Route.",
            },
          },
        ],
        adapterMetrics: {
          logicalClient: "orders",
          method: "stream",
          activeInvocations: 1n,
          acquisitionCount: 1n,
          reconnectCount: 0n,
          completionCount: 0n,
          streamFailureCount: 0n,
          messageCount: 5n,
          mappedMessageCount: 1n,
          rejectedMessageCount: 4n,
          cancellationCount: 0n,
          finalizationCount: 0n,
          finalizationFailureCount: 0n,
          activeFeeds: 1n,
        },
      });
      const encodedDegraded = yield* viewServerEncodeSourceHealth(config, "orders", degraded);
      expect(yield* viewServerDecodeSourceHealth(config, "orders", encodedDegraded)).toStrictEqual(
        degraded,
      );
      const encodedSampled = yield* viewServerEncodeSourceHealth(config, "orders", sampledActive);
      expect(yield* viewServerDecodeSourceHealth(config, "orders", encodedSampled)).toStrictEqual(
        sampledActive,
      );

      yield* subscription.close();
      yield* leaseKeeper.close();
      yield* availabilitySubscription.close();
      yield* Effect.forEach(rejectionDiagnostics, (entry) => entry.events.close(), {
        discard: true,
      });
      yield* diagnostics.close();
      yield* sampledDiagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("contains invocation and finalization failures in exact safe diagnostics", () =>
    Effect.gen(function* () {
      const invocationFailureClient = makeControlledClient({
        invocationFailureCause: new ConnectError(
          "private planned invocation failure",
          Code.Unavailable,
        ),
      });
      const source = grpc.topicSources({ orders: OrdersService }).materialized(
        {
          client: "orders",
          method: "stream",
          request: () => ({ region: "all" }),
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        },
        Schedule.recurs(0),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          grpcServerLayer(config, {
            orders: invocationFailureClient.client,
          }),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      const exhausted = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      if (exhausted.status._tag !== "Exhausted") {
        return yield* Effect.die("Expected exhausted Materialized diagnostics.");
      }
      expect(exhausted.status.exhaustion).toStrictEqual({
        _tag: "RetryExhausted",
        lastTermination: {
          _tag: "Failed",
          failure: {
            _tag: "AdapterFailure",
            failure: {
              _tag: "GrpcInvocationFailure",
              client: "orders",
              code: "UNAVAILABLE",
              message: "The selected gRPC server-streaming method could not be invoked.",
              method: "stream",
            },
          },
        },
      });
      expect(typeof exhausted.status.exhaustedAtNanos).toBe("bigint");
      const encodedExhausted = yield* viewServerEncodeSourceHealth(config, "orders", exhausted);
      expect(yield* viewServerDecodeSourceHealth(config, "orders", encodedExhausted)).toStrictEqual(
        exhausted,
      );
      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("contains hostile invocation and stream causes in typed UNKNOWN failures", () =>
    Effect.gen(function* () {
      const hostileCause = Object.defineProperty({}, Symbol.toPrimitive, {
        value: () => {
          throw new Error("private hostile cause coercion");
        },
      });
      for (const phase of ["invocation", "stream"] as const) {
        const controlled = makeControlledClient(
          phase === "invocation" ? { invocationFailureCause: hostileCause } : undefined,
        );
        const source = grpc.topicSources({ orders: OrdersService }).materialized(
          {
            client: "orders",
            method: "stream",
            request: () => ({ region: "all" }),
            map: ({ value }) => ({
              id: value.id,
              price: value.price,
              region: value.region,
            }),
          },
          Schedule.recurs(0),
        );
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source,
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        if (phase === "stream") {
          yield* awaitInvocationCount(controlled, 1);
          yield* controlled.fail(0, hostileCause);
        }
        const exhausted = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        if (exhausted.status._tag !== "Exhausted") {
          return yield* Effect.die(`Expected hostile ${phase} failure exhaustion.`);
        }
        expect(exhausted.status.exhaustion.lastTermination).toStrictEqual({
          _tag: "Failed",
          failure: {
            _tag: "AdapterFailure",
            failure: {
              _tag: phase === "invocation" ? "GrpcInvocationFailure" : "GrpcStreamFailure",
              client: "orders",
              code: "UNKNOWN",
              message:
                phase === "invocation"
                  ? "The selected gRPC server-streaming method could not be invoked."
                  : "The upstream gRPC response stream failed.",
              method: "stream",
            },
          },
        });
        yield* diagnostics.close();
        yield* runtime.close;
      }
    }),
  );

  it.effect("reports corrupted runtime definition and client lookups as typed failures", () =>
    Effect.gen(function* () {
      const source = grpc.topicSources({ orders: OrdersService }).materialized(
        {
          client: "orders",
          method: "stream",
          request: () => ({ region: "all" }),
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        },
        Schedule.recurs(0),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const bindingPlan = yield* grpcBindingPlan(config);
      const captured = Option.getOrThrow(
        Option.fromUndefinedOr(bindingPlan.definitions.get(source.options)),
      );
      const method = Option.getOrThrow(Option.fromUndefinedOr(bindingPlan.methods.get(captured)));

      const missingDefinitionRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          grpcServerLayerFromBindingPlan(
            {
              ...bindingPlan,
              definitions: new WeakMap<object, typeof captured>(),
            },
            { orders: makeControlledClient().client },
          ),
        ),
      );
      const missingDefinitionDiagnostics =
        yield* missingDefinitionRuntime.liveClient.subscribeSourceHealth({ topic: "orders" });
      const missingDefinitionHealth = Option.getOrThrow(
        yield* missingDefinitionDiagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      if (missingDefinitionHealth.status._tag !== "Exhausted") {
        return yield* Effect.die("Expected missing definition lookup exhaustion.");
      }
      expect(missingDefinitionHealth.status.exhaustion.lastTermination).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "GrpcConfigurationFailure",
            client: "orders",
            message: "The captured gRPC Source Definition is unavailable during client lookup.",
            phase: "client-lookup",
          },
        },
      });
      yield* missingDefinitionDiagnostics.close();
      yield* missingDefinitionRuntime.close;

      const missingClientDefinition = Object.freeze({
        ...captured,
        client: "missing",
      });
      const missingClientRuntime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          grpcServerLayerFromBindingPlan(
            {
              ...bindingPlan,
              definitions: new WeakMap([[source.options, missingClientDefinition]]),
              methods: new WeakMap([[missingClientDefinition, method]]),
            },
            { orders: makeControlledClient().client },
          ),
        ),
      );
      const missingClientDiagnostics = yield* missingClientRuntime.liveClient.subscribeSourceHealth(
        { topic: "orders" },
      );
      const missingClientHealth = Option.getOrThrow(
        yield* missingClientDiagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      if (missingClientHealth.status._tag !== "Exhausted") {
        return yield* Effect.die("Expected missing client lookup exhaustion.");
      }
      expect(missingClientHealth.status.exhaustion.lastTermination).toStrictEqual({
        _tag: "Failed",
        failure: {
          _tag: "AdapterFailure",
          failure: {
            _tag: "GrpcConfigurationFailure",
            client: "missing",
            message: "The configured gRPC runtime client is unavailable during client lookup.",
            phase: "client-lookup",
          },
        },
      });
      yield* missingClientDiagnostics.close();
      yield* missingClientRuntime.close;
    }),
  );

  it.effect("caches request-construction failures across retries", () =>
    Effect.gen(function* () {
      const controlled = makeControlledClient();
      let requestCalls = 0;
      const source = grpc.topicSources({ orders: OrdersService }).materialized(
        {
          client: "orders",
          method: "stream",
          request: (): { readonly region: string } => {
            requestCalls += 1;
            throw new Error("planned request failure");
          },
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        },
        Schedule.recurs(1),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      const exhausted = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      if (exhausted.status._tag !== "Exhausted") {
        return yield* Effect.die("Expected request failure exhaustion.");
      }

      expect({
        requestCalls,
        invocations: controlled.invocations.length,
        lastTermination: exhausted.status.exhaustion.lastTermination,
      }).toStrictEqual({
        requestCalls: 1,
        invocations: 0,
        lastTermination: {
          _tag: "Failed",
          failure: {
            _tag: "AdapterFailure",
            failure: {
              _tag: "GrpcRequestConstructionFailure",
              client: "orders",
              method: "stream",
              message:
                "The gRPC request factory threw or returned an invalid generated request-init object.",
            },
          },
        },
      });

      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );

  it.effect("rejects malformed AsyncIterable acquisition shapes and unexpected completion", () =>
    Effect.gen(function* () {
      const modes = [
        { invalidIterable: true },
        { throwingIterableAccessor: true },
        { iteratorFailure: true },
        { invalidIterator: true },
        { throwingIteratorAccessor: true },
      ];
      for (const mode of modes) {
        const controlled = makeControlledClient(mode);
        const source = grpc.topicSources({ orders: OrdersService }).materialized(
          {
            client: "orders",
            method: "stream",
            request: () => ({ region: "all" }),
            map: ({ value }) => ({
              id: value.id,
              price: value.price,
              region: value.region,
            }),
          },
          Schedule.recurs(0),
        );
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source,
            },
          },
        });
        const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
          Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
        );
        const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
        const exhausted = Option.getOrThrow(
          yield* diagnostics.events.pipe(
            Stream.filter((health) => health.status._tag === "Exhausted"),
            Stream.take(1),
            Stream.runHead,
          ),
        );
        if (exhausted.status._tag !== "Exhausted") {
          return yield* Effect.die("Expected malformed acquisition exhaustion.");
        }
        expect(exhausted.status.exhaustion.lastTermination).toStrictEqual({
          _tag: "Failed",
          failure: {
            _tag: "AdapterFailure",
            failure: {
              _tag: "GrpcInvocationFailure",
              client: "orders",
              code: "UNKNOWN",
              message: "The selected gRPC server-streaming method could not be invoked.",
              method: "stream",
            },
          },
        });
        yield* diagnostics.close();
        yield* runtime.close;
      }

      const completed = makeControlledClient();
      const completedSource = grpc.topicSources({ orders: OrdersService }).materialized(
        {
          client: "orders",
          method: "stream",
          request: () => ({ region: "all" }),
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        },
        Schedule.recurs(0),
      );
      const completedConfig = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source: completedSource,
          },
        },
      });
      const completedRuntime = yield* makeViewServerRuntimeCore(completedConfig, {}).pipe(
        Effect.provide(grpcServerLayer(completedConfig, { orders: completed.client })),
      );
      const completedDiagnostics = yield* completedRuntime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      yield* awaitInvocationCount(completed, 1);
      yield* completed.complete(0);
      const completedHealth = Option.getOrThrow(
        yield* completedDiagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      if (completedHealth.status._tag !== "Exhausted") {
        return yield* Effect.die("Expected completion exhaustion.");
      }
      expect(completedHealth.status.exhaustion.lastTermination).toStrictEqual({
        _tag: "UnexpectedCompletion",
      });
      yield* TestClock.adjust("1 second");
      const completedMetricsDiagnostics = yield* completedRuntime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      const completedSampled = Option.getOrThrow(
        yield* completedMetricsDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );
      expect(completedSampled.metrics.adapter).toStrictEqual({
        logicalClient: "orders",
        method: "stream",
        activeInvocations: 0n,
        acquisitionCount: 1n,
        reconnectCount: 0n,
        completionCount: 1n,
        streamFailureCount: 0n,
        messageCount: 0n,
        mappedMessageCount: 0n,
        rejectedMessageCount: 0n,
        cancellationCount: 0n,
        finalizationCount: 1n,
        finalizationFailureCount: 0n,
      });
      const encodedCompleted = yield* viewServerEncodeSourceHealth(
        completedConfig,
        "orders",
        completedSampled,
      );
      expect(
        yield* viewServerDecodeSourceHealth(completedConfig, "orders", encodedCompleted),
      ).toStrictEqual(completedSampled);
      yield* completedDiagnostics.close();
      yield* completedMetricsDiagnostics.close();
      yield* completedRuntime.close;
    }),
  );

  it.effect("makes iterator finalization idempotent and infallible for every return shape", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const modes = [
          { client: { omitIteratorReturn: true }, finalizationFailureCount: 0n, returns: 0 },
          { client: { invalidIteratorReturn: true }, finalizationFailureCount: 0n, returns: 1 },
          { client: { rejectFinalization: true }, finalizationFailureCount: 1n, returns: 1 },
        ];
        for (const mode of modes) {
          const controlled = makeControlledClient(mode.client);
          const source = grpc.topicSources({ orders: OrdersService }).materialized({
            client: "orders",
            method: "stream",
            request: () => ({ region: "all" }),
            map: ({ value }) => ({
              id: value.id,
              price: value.price,
              region: value.region,
            }),
          });
          const config = defineViewServerConfig({
            topics: {
              orders: {
                schema: Order,
                source,
              },
            },
          });
          const adapterContext = yield* Layer.build(
            grpcServerLayer(config, { orders: controlled.client }),
          );
          const adapterService = Context.get(adapterContext, GrpcSourceAdapter.runtimeService);
          const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
            Effect.provide(adapterContext),
          );
          const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
          const ready = Option.getOrThrow(
            yield* diagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "Ready"),
              Stream.take(1),
              Stream.runHead,
            ),
          );
          yield* awaitInvocationCount(controlled, 1);
          yield* diagnostics.close();
          yield* runtime.close;
          const lifecycle = Option.getOrThrow(Option.fromUndefinedOr(adapterService.materialized));
          const lifetimeScope = yield* Effect.scope;
          const metrics = yield* lifecycle.metrics({
            definition: source.options,
            lifetimeScope,
            target: ready.target,
            topic: "orders",
          });

          expect({
            aborted: controlled.invocations[0]?.signal.aborted,
            returns: controlled.invocations[0]?.returns,
            metrics,
          }).toStrictEqual({
            aborted: true,
            returns: mode.returns,
            metrics: {
              logicalClient: "orders",
              method: "stream",
              activeInvocations: 0n,
              acquisitionCount: 1n,
              reconnectCount: 0n,
              completionCount: 0n,
              streamFailureCount: 0n,
              messageCount: 0n,
              mappedMessageCount: 0n,
              rejectedMessageCount: 0n,
              cancellationCount: 1n,
              finalizationCount: 1n,
              finalizationFailureCount: mode.finalizationFailureCount,
            },
          });
        }
      }),
    ),
  );

  it.effect("records rejected finalization exactly once with safe structured diagnostics", () => {
    const logs: Array<{
      readonly annotations: Readonly<Record<string, unknown>>;
      readonly level: unknown;
      readonly message: unknown;
    }> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push({
        annotations: options.fiber.getRef(References.CurrentLogAnnotations),
        level: options.logLevel,
        message: options.message,
      });
    });
    return Effect.gen(function* () {
      const controlled = makeControlledClient({ rejectFinalization: true });
      const source = grpc.topicSources({ orders: OrdersService }).materialized(
        {
          client: "orders",
          method: "stream",
          request: () => ({ region: "all" }),
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        },
        Schedule.recurs(0),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      yield* awaitInvocationCount(controlled, 1);
      yield* controlled.fail(0);
      const exhausted = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );
      yield* TestClock.adjust("1 second");
      const metricsDiagnostics = yield* runtime.liveClient.subscribeSourceHealth({
        topic: "orders",
      });
      const sampled = Option.getOrThrow(
        yield* metricsDiagnostics.events.pipe(Stream.take(1), Stream.runHead),
      );

      expect({
        aborted: controlled.invocations[0]?.signal.aborted,
        returns: controlled.invocations[0]?.returns,
        status: exhausted.status._tag,
        adapterMetrics: sampled.metrics.adapter,
        logs,
      }).toStrictEqual({
        aborted: true,
        returns: 1,
        status: "Exhausted",
        adapterMetrics: {
          logicalClient: "orders",
          method: "stream",
          activeInvocations: 0n,
          acquisitionCount: 1n,
          reconnectCount: 0n,
          completionCount: 0n,
          streamFailureCount: 1n,
          messageCount: 0n,
          mappedMessageCount: 0n,
          rejectedMessageCount: 0n,
          cancellationCount: 0n,
          finalizationCount: 1n,
          finalizationFailureCount: 1n,
        },
        logs: [
          {
            annotations: {
              adapter: {
                name: "grpc",
                version: "1",
              },
              attempt: 1n,
              client: "orders",
              failure: {
                _tag: "GrpcCancellationFailure",
                client: "orders",
                method: "stream",
                message:
                  "The upstream gRPC invocation rejected cancellation or iterator finalization.",
              },
              lifecycle: "Materialized",
              method: "stream",
              topic: "orders",
            },
            level: "Warn",
            message: ["gRPC Source Attempt finalization failed."],
          },
        ],
      });

      yield* diagnostics.close();
      yield* metricsDiagnostics.close();
      yield* runtime.close;
      expect(controlled.invocations[0]?.returns).toBe(1);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("uses an opaque Leased Feed Route reference in finalization diagnostics", () => {
    const logs: Array<{
      readonly annotations: Readonly<Record<string, unknown>>;
      readonly message: unknown;
    }> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push({
        annotations: options.fiber.getRef(References.CurrentLogAnnotations),
        message: options.message,
      });
    });
    return Effect.gen(function* () {
      const controlled = makeControlledClient({ rejectFinalization: true });
      const source = grpc.topicSources({ orders: OrdersService }).leased({
        client: "orders",
        method: "stream",
        routeBy: ["region"],
        request: (route) => ({ region: route.region }),
        map: ({ value }) => ({
          id: value.id,
          price: value.price,
          region: value.region,
        }),
      });
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
      );
      const subscription = yield* runtime.liveClient.subscribe("orders", {
        routeBy: { region: "browser-identity-sentinel" },
        select: ["id"],
      });
      yield* awaitInvocationCount(controlled, 1);
      yield* subscription.close();
      yield* awaitCondition(
        () => `one Leased finalization diagnostic; last observed ${logs.length}`,
        () => logs.length === 1,
      );

      expect({
        containsRouteSentinel: JSON.stringify(logs, (_key, value) =>
          typeof value === "bigint" ? value.toString() : value,
        ).includes("browser-identity-sentinel"),
        logs,
      }).toStrictEqual({
        containsRouteSentinel: false,
        logs: [
          {
            annotations: {
              adapter: {
                name: "grpc",
                version: "1",
              },
              attempt: 1n,
              client: "orders",
              feedRoute: "leased-feed-1",
              failure: {
                _tag: "GrpcCancellationFailure",
                client: "orders",
                method: "stream",
                message:
                  "The upstream gRPC invocation rejected cancellation or iterator finalization.",
              },
              lifecycle: "Leased",
              method: "stream",
              topic: "orders",
            },
            message: ["gRPC Source Attempt finalization failed."],
          },
        ],
      });
      yield* runtime.close;
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("rejects tampered bindings and exact aggregate client maps before startup", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const baseSource = grpc.topicSources({ orders: OrdersService }).materialized({
          client: "orders",
          method: "stream",
          request: () => ({ region: "all" }),
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        });
        const invalidOptionsSource = rawMaterializedSource({});
        const nonObjectOptionsSource = rawMaterializedSource(null);
        const throwingServiceSource = rawMaterializedSource({
          ...baseSource.options,
          service: (): typeof OrdersService => {
            throw new Error("planned descriptor failure");
          },
        });
        const nonDescriptorServiceSource = rawMaterializedSource({
          ...baseSource.options,
          service: () => null,
        });
        const missingMethodSource = rawMaterializedSource({
          ...baseSource.options,
          method: "missing",
        });
        const conflictingSource = grpc.topicSources({ orders: InventoryService }).materialized({
          client: "orders",
          method: "stream",
          request: () => ({ region: "all" }),
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        });
        const hostileViewServer = {
          topics: new Proxy(
            {},
            {
              ownKeys: () => {
                throw new Error("planned View Server topics inspection failure");
              },
            },
          ),
        };
        const failures = yield* Effect.all([
          Effect.flip(grpcServiceBindings(hostileViewServer)),
          Effect.flip(grpcServiceBindings({ topics: { plain: {} } })),
          Effect.flip(
            grpcServiceBindings({
              topics: {
                invalid: { source: invalidOptionsSource },
              },
            }),
          ),
          Effect.flip(
            grpcServiceBindings({
              topics: {
                invalid: { source: nonObjectOptionsSource },
              },
            }),
          ),
          Effect.flip(
            grpcServiceBindings({
              topics: {
                throwing: { source: throwingServiceSource },
              },
            }),
          ),
          Effect.flip(
            grpcServiceBindings({
              topics: {
                nonDescriptor: { source: nonDescriptorServiceSource },
              },
            }),
          ),
          Effect.flip(
            grpcServiceBindings({
              topics: {
                missingMethod: { source: missingMethodSource },
              },
            }),
          ),
          Effect.flip(
            grpcServiceBindings({
              topics: {
                orders: { source: baseSource },
                inventory: { source: conflictingSource },
              },
            }),
          ),
        ]);
        expect(failures.map((failure) => failure._tag)).toStrictEqual([
          "GrpcConfigurationFailure",
          "GrpcConfigurationFailure",
          "GrpcConfigurationFailure",
          "GrpcConfigurationFailure",
          "GrpcConfigurationFailure",
          "GrpcConfigurationFailure",
          "GrpcConfigurationFailure",
          "GrpcConfigurationFailure",
        ]);
        expect(failures[0]).toStrictEqual({
          _tag: "GrpcConfigurationFailure",
          client: "",
          message: "The View Server gRPC Source Definitions could not be inspected safely.",
          phase: "configuration",
        });
        const config = defineViewServerConfig({
          topics: {
            orders: {
              schema: Order,
              source: baseSource,
            },
          },
        });
        const wrongServiceExit = yield* Layer.build(
          grpcServerLayer(config, {
            orders: {
              service: InventoryService,
              invoke: () => ({}),
            },
          }),
        ).pipe(Effect.exit);
        const malformedLayer = Reflect.apply(grpcServerLayer, undefined, [
          config,
          {
            orders: {
              service: OrdersService,
              invoke: "invalid",
            },
          },
        ]);
        if (!isClosedLayer(malformedLayer)) {
          return yield* Effect.die("Expected reflected gRPC server Layer.");
        }
        const malformedExit = yield* Layer.build(malformedLayer).pipe(Effect.exit);
        const extraExit = yield* Layer.build(
          grpcServerLayer(config, {
            orders: makeControlledClient().client,
            extra: makeControlledClient().client,
          }),
        ).pipe(Effect.exit);
        let accessorCalls = 0;
        const accessorClients = {};
        Object.defineProperty(accessorClients, "orders", {
          enumerable: true,
          get: () => {
            accessorCalls += 1;
            return makeControlledClient().client;
          },
        });
        const accessorRuntime = {};
        Object.defineProperty(accessorRuntime, "service", {
          enumerable: true,
          get: () => {
            accessorCalls += 1;
            return OrdersService;
          },
        });
        Object.defineProperty(accessorRuntime, "invoke", {
          enumerable: true,
          value: () => ({}),
        });
        const hostileClientMaps: ReadonlyArray<unknown> = [
          null,
          [],
          new (class RuntimeClients {
            readonly orders = makeControlledClient().client;
          })(),
          { [Symbol("orders")]: makeControlledClient().client },
          accessorClients,
          { orders: accessorRuntime },
          new Proxy(
            {},
            {
              ownKeys: () => {
                throw new Error("planned client-map proxy failure");
              },
            },
          ),
        ];
        const hostileExits: Array<Exit.Exit<unknown, unknown>> = [];
        for (const clients of hostileClientMaps) {
          const layer = Reflect.apply(grpcServerLayer, undefined, [config, clients]);
          if (!isClosedLayer(layer)) {
            return yield* Effect.die("Expected hostile gRPC server Layer.");
          }
          hostileExits.push(yield* Layer.build(layer).pipe(Effect.exit));
        }

        expect({
          wrongService: Exit.isFailure(wrongServiceExit),
          malformed: Exit.isFailure(malformedExit),
          extra: Exit.isFailure(extraExit),
          hostileFailures: hostileExits.map(Exit.findErrorOption),
          accessorCalls,
        }).toStrictEqual({
          wrongService: true,
          malformed: true,
          extra: true,
          hostileFailures: hostileExits.map(() =>
            Option.some({
              _tag: "GrpcConfigurationFailure",
              client: "",
              message:
                "The gRPC aggregate client map must contain exact data-only runtime clients.",
              phase: "client-construction",
            }),
          ),
          accessorCalls: 0,
        });
      }),
    ),
  );

  it.effect("resolves each generated service descriptor once before Source Attempts start", () =>
    Effect.gen(function* () {
      type Row = {
        readonly id: string;
        readonly price: number;
        readonly region: string;
      };
      let serviceCalls = 0;
      const source = GrpcSourceAdapter.materializedSource<
        Row,
        GrpcMaterializedDefinitionOptions<string, Row>,
        never
      >(
        {
          client: "orders",
          mapValue: () => ({
            id: "one",
            price: 1,
            region: "eu",
          }),
          method: "stream",
          request: () => ({ region: "all" }),
          service: () => {
            serviceCalls += 1;
            if (serviceCalls > 1) {
              throw new Error("service descriptor was resolved more than once");
            }
            return OrdersService;
          },
        },
        undefined,
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const controlled = makeControlledClient();
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(grpcServerLayer(config, { orders: controlled.client })),
      );
      yield* awaitInvocationCount(controlled, 1);

      expect({
        requestRegion: requestField(controlled.invocations[0]?.request, "region"),
        serviceCalls,
      }).toStrictEqual({
        requestRegion: "all",
        serviceCalls: 1,
      });

      yield* runtime.close;
    }),
  );

  it.effect("reports a typed failure when a validated method plan is tampered", () =>
    Effect.gen(function* () {
      const source = grpc.topicSources({ orders: OrdersService }).materialized(
        {
          client: "orders",
          method: "stream",
          request: () => ({ region: "all" }),
          map: ({ value }) => ({
            id: value.id,
            price: value.price,
            region: value.region,
          }),
        },
        Schedule.recurs(0),
      );
      const config = defineViewServerConfig({
        topics: {
          orders: {
            schema: Order,
            source,
          },
        },
      });
      const bindingPlan = yield* grpcBindingPlan(config);
      const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
        Effect.provide(
          grpcServerLayerFromBindingPlan(
            {
              definitions: bindingPlan.definitions,
              services: bindingPlan.services,
              methods: new WeakMap(),
            },
            { orders: makeControlledClient().client },
          ),
        ),
      );
      const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
      const exhausted = Option.getOrThrow(
        yield* diagnostics.events.pipe(
          Stream.filter((health) => health.status._tag === "Exhausted"),
          Stream.take(1),
          Stream.runHead,
        ),
      );

      expect(exhausted.status).toStrictEqual({
        _tag: "Exhausted",
        exhaustedAtNanos: 0n,
        exhaustion: {
          _tag: "RetryExhausted",
          lastTermination: {
            _tag: "Failed",
            failure: {
              _tag: "AdapterFailure",
              failure: {
                _tag: "GrpcConfigurationFailure",
                client: "orders",
                message:
                  "The validated gRPC method binding is unavailable for this Source Definition.",
                phase: "configuration",
              },
            },
          },
        },
      });

      yield* diagnostics.close();
      yield* runtime.close;
    }),
  );
});
