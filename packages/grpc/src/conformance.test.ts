import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import {
  registerSourceAdapterConformance,
  SourceAdapterConformanceDriver,
  type SourceAdapterConformanceCommand,
  type SourceAdapterConformanceRow,
  type SourceAdapterConformanceTarget,
  type SourceAdapterConformanceTransportObservation,
} from "@effect-view-server/source-adapter-conformance-host";
import { makeSourceAdapterConformanceDriver } from "@effect-view-server/source-adapter-testing";
import { Effect, Layer, Schedule, Stream } from "effect";
import { grpc, GrpcSourceAdapter, type GrpcAdapterFailure } from "./model";
import { grpcServerLayer, type GrpcRuntimeClient, type GrpcServerViewServer } from "./server";

type RequestMessage = Message<"grpc.conformance.Request"> & {
  readonly region: string;
};

type EventMessage = Message<"grpc.conformance.Event"> & {
  readonly id: string;
  readonly region: string;
  readonly value: string;
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/conformance.proto",
          package: "grpc.conformance",
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
                  type: FieldDescriptorProto_Type.STRING,
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
                  inputType: ".grpc.conformance.Request",
                  outputType: ".grpc.conformance.Event",
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

type StreamCommand =
  | {
      readonly _tag: "Value";
      readonly value: SourceAdapterConformanceRow;
    }
  | {
      readonly _tag: "Reject";
      readonly region: string;
    }
  | {
      readonly _tag: "Fail";
    }
  | {
      readonly _tag: "Complete";
    };

type Pending = {
  readonly resolve: (result: IteratorResult<unknown>) => void;
  readonly reject: (error: unknown) => void;
};

type Invocation = {
  readonly key: string;
  readonly commands: Array<StreamCommand>;
  readonly pending: Array<Pending>;
  closed: boolean;
};

type TargetState = {
  acquisitions: bigint;
  finalizations: bigint;
  partialAcquisitionFinalizations: bigint;
  failNextAcquisition: "none" | "invoke" | "iterator";
  blockNextFinalizer: boolean;
  finalizerStarted: boolean;
  readonly releaseFinalizers: Array<() => void>;
  readonly invocations: Array<Invocation>;
};

const materializedRequestRegion = "__materialized__";

const targetKey = (target: SourceAdapterConformanceTarget): string =>
  target._tag === "Materialized" ? "materialized" : `leased:${String(target.route["region"])}`;

const requestKey = (request: unknown): string => {
  const region =
    typeof request === "object" && request !== null ? Reflect.get(request, "region") : undefined;
  return region === materializedRequestRegion ? "materialized" : `leased:${String(region)}`;
};

const targetRegion = (target: SourceAdapterConformanceTarget): string =>
  target._tag === "Materialized" ? "eu" : String(target.route["region"]);

const makeGrpcConformanceDriver = Effect.suspend(() => {
  const states = new Map<string, TargetState>();
  const stateFor = (key: string): TargetState => {
    const current = states.get(key);
    if (current !== undefined) {
      return current;
    }
    const created: TargetState = {
      acquisitions: 0n,
      finalizations: 0n,
      partialAcquisitionFinalizations: 0n,
      failNextAcquisition: "none",
      blockNextFinalizer: false,
      finalizerStarted: false,
      releaseFinalizers: [],
      invocations: [],
    };
    states.set(key, created);
    return created;
  };
  const activeInvocation = (target: SourceAdapterConformanceTarget): Invocation => {
    const invocations = stateFor(targetKey(target)).invocations;
    const active = invocations.findLast((invocation) => !invocation.closed);
    if (active === undefined) {
      throw new TypeError("The controllable gRPC conformance stream is not active.");
    }
    return active;
  };
  const applyCommand = (invocation: Invocation, command: StreamCommand): void => {
    const pending = invocation.pending.shift();
    if (pending === undefined) {
      invocation.commands.push(command);
      return;
    }
    if (command._tag === "Value") {
      pending.resolve({ done: false, value: command.value });
      return;
    }
    if (command._tag === "Reject") {
      pending.resolve({
        done: false,
        value: {
          id: "rejected",
          region: command.region,
          value: "__reject__",
        },
      });
      return;
    }
    if (command._tag === "Complete") {
      pending.resolve({ done: true, value: undefined });
      return;
    }
    pending.reject(new Error("planned gRPC conformance stream failure"));
  };
  const takeCommand = (invocation: Invocation): Promise<IteratorResult<unknown>> => {
    const command = invocation.commands.shift();
    if (command === undefined) {
      return new Promise((resolve, reject) => {
        invocation.pending.push({ resolve, reject });
      });
    }
    if (command._tag === "Value") {
      return Promise.resolve({ done: false, value: command.value });
    }
    if (command._tag === "Reject") {
      return Promise.resolve({
        done: false,
        value: {
          id: "rejected",
          region: command.region,
          value: "__reject__",
        },
      });
    }
    if (command._tag === "Complete") {
      return Promise.resolve({ done: true, value: undefined });
    }
    return Promise.reject(new Error("planned gRPC conformance stream failure"));
  };
  const client: GrpcRuntimeClient = {
    service: RowsService,
    invoke: (_method, request, signal) => {
      const key = requestKey(request);
      const state = stateFor(key);
      if (state.failNextAcquisition === "invoke") {
        state.failNextAcquisition = "none";
        throw new Error("planned gRPC conformance acquisition failure");
      }
      const invocation: Invocation = {
        key,
        commands: [],
        pending: [],
        closed: false,
      };
      return {
        [Symbol.asyncIterator]: () => {
          if (state.failNextAcquisition === "iterator") {
            state.failNextAcquisition = "none";
            signal.addEventListener(
              "abort",
              () => {
                state.partialAcquisitionFinalizations += 1n;
              },
              { once: true },
            );
            throw new Error("planned gRPC conformance iterator acquisition failure");
          }
          state.acquisitions += 1n;
          state.invocations.push(invocation);
          return {
            next: () => takeCommand(invocation),
            return: () => {
              if (invocation.closed) {
                return Promise.resolve({ done: true, value: undefined });
              }
              invocation.closed = true;
              state.finalizerStarted = true;
              const pending = invocation.pending.splice(0);
              for (const waiter of pending) {
                waiter.resolve({ done: true, value: undefined });
              }
              const finish = (): void => {
                state.finalizations += 1n;
                state.finalizerStarted = false;
              };
              if (state.blockNextFinalizer) {
                state.blockNextFinalizer = false;
                return new Promise((resolve) => {
                  state.releaseFinalizers.push(() => {
                    finish();
                    resolve({ done: true, value: undefined });
                  });
                });
              }
              finish();
              return Promise.resolve({ done: true, value: undefined });
            },
          };
        },
      };
    },
  };

  const sources = grpc.topicSources({
    rows: RowsService,
  });
  const mapping = (input: { readonly value: EventMessage }): SourceAdapterConformanceRow => {
    if (input.value.value === "__reject__") {
      throw new Error("planned gRPC conformance Mapping rejection");
    }
    return {
      id: input.value.id,
      region: input.value.region,
      value: input.value.value,
    };
  };
  const leasedMapping = (input: {
    readonly value: EventMessage;
    readonly route: {
      readonly region: string;
    };
  }): SourceAdapterConformanceRow => {
    if (input.value.value === "__reject__") {
      throw new Error("planned gRPC conformance Mapping rejection");
    }
    return {
      id: input.value.id,
      region: input.route.region,
      value: input.value.value,
    };
  };
  const materializedSource = sources.materialized({
    client: "rows",
    method: "stream",
    request: () => ({ region: materializedRequestRegion }),
    map: mapping,
  });
  const materializedDelayedRetrySource = sources.materialized(
    {
      client: "rows",
      method: "stream",
      request: () => ({ region: materializedRequestRegion }),
      map: mapping,
    },
    Schedule.spaced("1 second").pipe(Schedule.upTo({ times: 1 })),
  );
  const materializedSingleRetrySource = sources.materialized(
    {
      client: "rows",
      method: "stream",
      request: () => ({ region: materializedRequestRegion }),
      map: mapping,
    },
    Schedule.recurs(1),
  );
  const leasedSource = sources.leased({
    client: "rows",
    method: "stream",
    routeBy: ["region"],
    request: (route) => ({ region: route.region }),
    map: leasedMapping,
  });
  const leasedDelayedRetrySource = sources.leased(
    {
      client: "rows",
      method: "stream",
      routeBy: ["region"],
      request: (route) => ({ region: route.region }),
      map: leasedMapping,
    },
    Schedule.spaced("1 second").pipe(Schedule.upTo({ times: 1 })),
  );
  const leasedSingleRetrySource = sources.leased(
    {
      client: "rows",
      method: "stream",
      routeBy: ["region"],
      request: (route) => ({ region: route.region }),
      map: leasedMapping,
    },
    Schedule.recurs(1),
  );
  const viewServer: GrpcServerViewServer = {
    topics: {
      materialized: { source: materializedSource },
      materializedDelayed: { source: materializedDelayedRetrySource },
      materializedSingle: { source: materializedSingleRetrySource },
      leased: { source: leasedSource },
      leasedDelayed: { source: leasedDelayedRetrySource },
      leasedSingle: { source: leasedSingleRetrySource },
    },
  };
  const runtimeLayer = grpcServerLayer(viewServer, {
    rows: client,
  });
  const observation = (
    target: SourceAdapterConformanceTarget,
  ): SourceAdapterConformanceTransportObservation => {
    const state = stateFor(targetKey(target));
    return {
      acquisitions: state.acquisitions,
      finalizations: state.finalizations,
      partialAcquisitionFinalizations: state.partialAcquisitionFinalizations,
      registrations: 0n,
      callbackFinalizations: 0n,
      finalizerStarted: state.finalizerStarted,
    };
  };
  const unsupported = (input: SourceAdapterConformanceCommand) =>
    Effect.die(
      new TypeError(`The continuous-upserts gRPC transport cannot produce ${input._tag}.`),
    );
  const command = (input: SourceAdapterConformanceCommand): Effect.Effect<void, unknown> =>
    Effect.suspend(() => {
      if (input._tag === "Delivery") {
        const invocation = activeInvocation(input.target);
        for (const mutation of input.mutations) {
          if (mutation._tag !== "Upsert") {
            return unsupported(input);
          }
          applyCommand(invocation, {
            _tag: "Value",
            value: mutation.row,
          });
        }
        return Effect.void;
      }
      if (input._tag === "Reject") {
        applyCommand(activeInvocation(input.target), {
          _tag: "Reject",
          region: targetRegion(input.target),
        });
        return Effect.void;
      }
      if (input._tag === "FailLane") {
        applyCommand(activeInvocation(input.target), { _tag: "Fail" });
        return Effect.void;
      }
      if (input._tag === "CompleteLane") {
        applyCommand(activeInvocation(input.target), { _tag: "Complete" });
        return Effect.void;
      }
      if (input._tag === "FailNextAcquisition") {
        stateFor(targetKey(input.target)).failNextAcquisition = input.afterFirstResource
          ? "iterator"
          : "invoke";
        return Effect.void;
      }
      if (input._tag === "SetMetrics") {
        if (input.sample === "invalid") {
          return unsupported(input);
        }
        if (input.sample === "updated") {
          for (const state of states.values()) {
            const invocation = state.invocations.findLast((candidate) => !candidate.closed);
            if (invocation !== undefined) {
              applyCommand(invocation, {
                _tag: "Value",
                value: {
                  id: "metric-sample",
                  region: invocation.key === "materialized" ? "eu" : invocation.key.slice(7),
                  value: "valid",
                },
              });
            }
          }
        }
        // A reset sample deliberately leaves this transport's immutable logical metrics unchanged.
        return Effect.void;
      }
      if (input._tag === "BlockNextFinalizer") {
        stateFor(targetKey(input.target)).blockNextFinalizer = true;
        return Effect.void;
      }
      if (input._tag === "ReleaseFinalizer") {
        const state = stateFor(targetKey(input.target));
        const releases = state.releaseFinalizers.splice(0);
        for (const release of releases) {
          release();
        }
        return Effect.void;
      }
      return unsupported(input);
    });
  const grpcMappingFailure: GrpcAdapterFailure = {
    _tag: "GrpcMappingFailure",
    client: "rows",
    method: "stream",
    message: "The synchronous gRPC Mapping callback threw.",
  };
  const grpcStreamFailure: GrpcAdapterFailure = {
    _tag: "GrpcStreamFailure",
    client: "rows",
    method: "stream",
    code: "UNKNOWN",
    message: "The upstream gRPC response stream failed.",
  };
  return Effect.succeed(
    makeSourceAdapterConformanceDriver({
      adapter: GrpcSourceAdapter,
      expectations: {
        materialized: {
          acquisitionFailure: {
            _tag: "GrpcInvocationFailure",
            client: "rows",
            method: "stream",
            code: "UNKNOWN",
            message: "The selected gRPC server-streaming method could not be invoked.",
          },
          partialAcquisitionFinalizationCount: 1n,
          streamFailure: grpcStreamFailure,
          settlementFailure: grpcStreamFailure,
          rejectionFailure: () => grpcMappingFailure,
          rejectionLocation: (_target, offset) => ({
            client: "rows",
            method: "stream",
            phase: "mapping",
            streamItemIndex: offset,
          }),
          updatedMetrics: {
            logicalClient: "rows",
            method: "stream",
            activeInvocations: 1n,
            acquisitionCount: 1n,
            reconnectCount: 0n,
            completionCount: 0n,
            streamFailureCount: 0n,
            messageCount: 1n,
            mappedMessageCount: 1n,
            rejectedMessageCount: 0n,
            cancellationCount: 0n,
            finalizationCount: 0n,
            finalizationFailureCount: 0n,
          },
        },
        leased: {
          acquisitionFailure: {
            _tag: "GrpcInvocationFailure",
            client: "rows",
            method: "stream",
            code: "UNKNOWN",
            message: "The selected gRPC server-streaming method could not be invoked.",
          },
          partialAcquisitionFinalizationCount: 1n,
          streamFailure: grpcStreamFailure,
          settlementFailure: grpcStreamFailure,
          rejectionFailure: () => grpcMappingFailure,
          rejectionLocation: (_target, offset) => ({
            client: "rows",
            method: "stream",
            phase: "mapping",
            streamItemIndex: offset,
          }),
          updatedMetrics: {
            logicalClient: "rows",
            method: "stream",
            activeInvocations: 1n,
            acquisitionCount: 1n,
            reconnectCount: 0n,
            completionCount: 0n,
            streamFailureCount: 0n,
            messageCount: 1n,
            mappedMessageCount: 1n,
            rejectedMessageCount: 0n,
            cancellationCount: 0n,
            finalizationCount: 0n,
            finalizationFailureCount: 0n,
            activeFeeds: 1n,
          },
        },
      },
      transport: {
        command,
        observe: (target) => Effect.suspend(() => Effect.succeed(observation(target))),
        changes: (target) =>
          Stream.fromEffectRepeat(Effect.yieldNow.pipe(Effect.map(() => observation(target)))),
      },
      runtimeLayer,
      materialized: {
        source: materializedSource,
        delayedRetrySource: materializedDelayedRetrySource,
        singleRetrySource: materializedSingleRetrySource,
      },
      leased: {
        source: leasedSource,
        delayedRetrySource: leasedDelayedRetrySource,
        singleRetrySource: leasedSingleRetrySource,
        sameRoute: {
          region: "eu",
        },
        distinctRoute: {
          region: "us",
        },
      },
    }),
  );
});

const driverLayer = Layer.effect(SourceAdapterConformanceDriver, makeGrpcConformanceDriver);

registerSourceAdapterConformance({
  name: "gRPC Materialized Source Adapter conformance",
  layer: driverLayer,
  materialized: true,
  eventModel: "continuous-upserts",
});

registerSourceAdapterConformance({
  name: "gRPC Leased Source Adapter conformance",
  layer: driverLayer,
  leased: true,
  eventModel: "continuous-upserts",
});
