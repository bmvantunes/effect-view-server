import { describe, expect, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { defineViewServerConfig, grpc, kafka } from "@effect-view-server/config";
import { Config, Deferred, Effect, Exit, Fiber, Logger, Schema, Stream } from "effect";
import {
  makeDefaultRuntimeDependencies,
  makeViewServerRuntimeWithDependencies,
  runViewServerRuntimeWithDependencies,
} from "./internal";
import type { ViewServerRuntimeDependencies } from "./runtime-dependencies";
import { ViewServerKafkaIngressError } from "./kafka-ingress";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const kafkaRegions = {
  local: "localhost:9092",
};

const kafkaViewServer = defineViewServerConfig({
  kafka: kafkaRegions,
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["local"],
        value: kafka.json(Order),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
        }),
      }),
    },
  },
});

type GrpcLifecycleValue = Message<"viewserver.lifecycle.Value"> & {
  readonly id: string;
  readonly route: string;
  readonly price: number;
};

type GrpcLifecycleRequest = Message<"viewserver.lifecycle.Request"> & {
  readonly route: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const lifecycleProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/lifecycle.proto",
        package: "viewserver.lifecycle",
        syntax: "proto3",
        messageType: [
          {
            name: "Value",
            field: [
              { name: "id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "route", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "Request",
            field: [{ name: "route", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
        ],
        service: [
          {
            name: "LifecycleService",
            method: [
              {
                name: "StreamRows",
                inputType: ".viewserver.lifecycle.Request",
                outputType: ".viewserver.lifecycle.Value",
                serverStreaming: true,
              },
            ],
          },
        ],
      }),
    ),
  ),
);

const lifecycleValueSchema = messageDesc<GrpcLifecycleValue>(lifecycleProtoFile, 0);
const lifecycleRequestSchema = messageDesc<GrpcLifecycleRequest>(lifecycleProtoFile, 1);
const lifecycleGrpcService = serviceDesc<{
  readonly streamRows: {
    readonly input: typeof lifecycleRequestSchema;
    readonly output: typeof lifecycleValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(lifecycleProtoFile, 0);

const GrpcLifecycleRow = Schema.Struct({
  id: Schema.String,
  route: Schema.String,
  price: Schema.Number,
});

const lifecycleGrpcClients = {
  rows: grpc.connectClient({
    service: lifecycleGrpcService,
    baseUrl: Config.succeed("https://lifecycle.example.test"),
  }),
};

const lifecycleGrpcTopics = grpc.topicSources(lifecycleGrpcClients);

const allSourceViewServer = defineViewServerConfig({
  kafka: kafkaRegions,
  grpc: {
    clients: lifecycleGrpcClients,
  },
  topics: {
    directOrders: {
      schema: Order,
      key: "id",
    },
    kafkaOrders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "orders-source",
        regions: ["local"],
        value: kafka.json(Order),
        key: kafka.stringKey(),
        rowKey: ({ key }) => key,
        map: ({ value }) => ({
          price: value.price,
        }),
      }),
    },
    materializedOrders: lifecycleGrpcTopics.materialized({
      schema: GrpcLifecycleRow,
      key: "id",
      client: "rows",
      method: "streamRows",
      request: () => ({ route: "all" }),
      acquire: () => Stream.never,
      map: ({ value }) => ({
        id: value.id,
        route: value.route,
        price: value.price,
      }),
    }),
    leasedOrders: lifecycleGrpcTopics.leased({
      schema: GrpcLifecycleRow,
      key: "id",
      client: "rows",
      method: "streamRows",
      routeBy: ["route"],
      request: ({ route }) => ({ route }),
      acquire: () => Stream.never,
      map: ({ value, route }) => ({
        id: value.id,
        route: route.route,
        price: value.price,
      }),
    }),
  },
});

const allSourceRuntimeOptions = {
  tcpPublishPort: 0,
  kafka: {
    consumerGroupId: "runtime-lifecycle-all-sources",
  },
};

const makeTrackedAllSourceDependencies = (
  events: Array<string>,
): ViewServerRuntimeDependencies<typeof allSourceViewServer.topics> => {
  const defaults = makeDefaultRuntimeDependencies<typeof allSourceViewServer.topics>();
  return {
    ...defaults,
    makeRuntimeCore: (config, options) =>
      Effect.sync(() => {
        events.push("acquire:runtimeCore");
      }).pipe(
        Effect.andThen(defaults.makeRuntimeCore(config, options)),
        Effect.map((runtimeCore) => ({
          ...runtimeCore,
          close: Effect.sync(() => {
            events.push("close:runtimeCore");
          }).pipe(Effect.andThen(runtimeCore.close)),
        })),
      ),
    makeGrpcLeaseManager: (...args) =>
      Effect.sync(() => {
        events.push("acquire:grpcLeaseManager");
      }).pipe(
        Effect.andThen(defaults.makeGrpcLeaseManager(...args)),
        Effect.map((manager) => ({
          ...manager,
          close: Effect.sync(() => {
            events.push("close:grpcLeaseManager");
          }).pipe(Effect.andThen(manager.close)),
        })),
      ),
    makeServer: () =>
      Effect.sync(() => {
        events.push("acquire:server");
        return {
          url: "ws://127.0.0.1:0/rpc",
          healthUrl: "http://127.0.0.1:0/health",
          metricsUrl: "http://127.0.0.1:0/metrics",
          close: Effect.sync(() => {
            events.push("close:server");
          }),
        };
      }),
    makeKafkaIngress: () =>
      Effect.sync(() => {
        events.push("acquire:kafkaIngress");
        return {
          close: Effect.sync(() => {
            events.push("close:kafkaIngress");
          }),
        };
      }),
    makeGrpcIngress: () =>
      Effect.sync(() => {
        events.push("acquire:grpcIngress");
        return {
          close: Effect.sync(() => {
            events.push("close:grpcIngress");
          }),
        };
      }),
    makeTcpPublishIngress: () =>
      Effect.sync(() => {
        events.push("acquire:tcpPublishIngress");
        return {
          url: "tcp://127.0.0.1:0",
          close: Effect.sync(() => {
            events.push("close:tcpPublishIngress");
          }),
        };
      }),
  };
};

const failCheckpoint = (events: Array<string>, checkpoint: string) =>
  Effect.sync(() => {
    events.push(`acquire:${checkpoint}`);
  }).pipe(Effect.andThen(Effect.die(new Error(`${checkpoint} startup failed`))));

describe("Real View Server composition lifecycle", () => {
  it.effect("acquires ingress Adapters in dependency order and finalizes them in reverse", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const dependencies = makeTrackedAllSourceDependencies(events);

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      );
      yield* runtime.close;

      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "acquire:kafkaIngress",
        "acquire:grpcIngress",
        "acquire:tcpPublishIngress",
        "close:tcpPublishIngress",
        "close:grpcIngress",
        "close:kafkaIngress",
        "close:server",
        "close:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("shares one join-idempotent close across every public owner", () =>
    Effect.gen(function* () {
      const cleanupStarted = yield* Deferred.make<void>();
      const allowCleanup = yield* Deferred.make<void>();
      let serverCloseCount = 0;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Deferred.succeed(cleanupStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowCleanup)),
              Effect.andThen(
                Effect.sync(() => {
                  serverCloseCount += 1;
                }),
              ),
            ),
          }),
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer);

      expect(runtime.liveClient.close).toBe(runtime.close);
      const firstClose = yield* runtime.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cleanupStarted);
      const secondClose = yield* runtime.liveClient.close.pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;

      const secondCloseBeforeRelease = secondClose.pollUnsafe();
      yield* Deferred.succeed(allowCleanup, undefined);
      yield* Fiber.join(firstClose);
      yield* Fiber.join(secondClose);
      expect(secondCloseBeforeRelease).toBeUndefined();
      yield* runtime.close;
      const health = yield* runtime.client.health();

      expect(serverCloseCount).toBe(1);
      expect(health.status).toBe("stopping");
    }),
  );

  it.effect("preserves the startup error while every acquired Adapter cleanup completes", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const startupError = new ViewServerKafkaIngressError({
        message: "Kafka startup failed",
        cause: "test checkpoint",
      });
      const defaults = makeDefaultRuntimeDependencies<typeof kafkaViewServer.topics>();
      const dependencies: ViewServerRuntimeDependencies<typeof kafkaViewServer.topics> = {
        ...defaults,
        makeRuntimeCore: (config, options) =>
          defaults.makeRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    events.push("close:runtimeCore");
                  }),
                ),
                Effect.andThen(Effect.die(new Error("runtime core cleanup defect"))),
              ),
            })),
          ),
        makeServer: () =>
          Effect.sync(() => {
            events.push("acquire:server");
            return {
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              metricsUrl: "http://127.0.0.1:0/metrics",
              close: Effect.sync(() => {
                events.push("close:server");
              }).pipe(Effect.andThen(Effect.die(new Error("server cleanup defect")))),
            };
          }),
        makeKafkaIngress: () =>
          Effect.sync(() => {
            events.push("acquire:kafkaIngress");
          }).pipe(Effect.andThen(Effect.fail(startupError))),
      };

      const observedError = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        kafkaViewServer,
        {
          kafka: {
            consumerGroupId: "runtime-lifecycle-test",
          },
        },
      ).pipe(Effect.flip);

      expect(observedError).toBe(startupError);
      expect(events).toStrictEqual([
        "acquire:server",
        "acquire:kafkaIngress",
        "close:server",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("does not finalize a Runtime Core whose acquisition fails", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const dependencies = {
        ...makeTrackedAllSourceDependencies(events),
        makeRuntimeCore: () => failCheckpoint(events, "runtimeCore"),
      };

      const startupExit = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.exit);

      expect(Exit.hasDies(startupExit)).toBe(true);
      expect(events).toStrictEqual(["acquire:runtimeCore"]);
    }),
  );

  it.effect("rolls back Runtime Core when gRPC lease-manager acquisition fails", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const dependencies = {
        ...makeTrackedAllSourceDependencies(events),
        makeGrpcLeaseManager: () => failCheckpoint(events, "grpcLeaseManager"),
      };

      const startupExit = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.exit);

      expect(Exit.hasDies(startupExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("rolls back the lease manager and Runtime Core when server acquisition fails", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const dependencies = {
        ...makeTrackedAllSourceDependencies(events),
        makeServer: () => failCheckpoint(events, "server"),
      };

      const startupExit = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.exit);

      expect(Exit.hasDies(startupExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "close:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("rolls back server ownership when Kafka acquisition fails", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const dependencies = {
        ...makeTrackedAllSourceDependencies(events),
        makeKafkaIngress: () => failCheckpoint(events, "kafkaIngress"),
      };

      const startupExit = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.exit);

      expect(Exit.hasDies(startupExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "acquire:kafkaIngress",
        "close:server",
        "close:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("rolls back Kafka ownership when materialized gRPC acquisition fails", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const dependencies = {
        ...makeTrackedAllSourceDependencies(events),
        makeGrpcIngress: () => failCheckpoint(events, "grpcIngress"),
      };

      const startupExit = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.exit);

      expect(Exit.hasDies(startupExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "acquire:kafkaIngress",
        "acquire:grpcIngress",
        "close:kafkaIngress",
        "close:server",
        "close:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("rolls back materialized gRPC ownership when TCP acquisition fails", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const dependencies = {
        ...makeTrackedAllSourceDependencies(events),
        makeTcpPublishIngress: () => failCheckpoint(events, "tcpPublishIngress"),
      };

      const startupExit = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.exit);

      expect(Exit.hasDies(startupExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "acquire:kafkaIngress",
        "acquire:grpcIngress",
        "acquire:tcpPublishIngress",
        "close:grpcIngress",
        "close:kafkaIngress",
        "close:server",
        "close:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("interrupts pending startup and finalizes every completed acquisition", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const tcpStartupStarted = yield* Deferred.make<void>();
      const dependencies = {
        ...makeTrackedAllSourceDependencies(events),
        makeTcpPublishIngress: () =>
          Deferred.succeed(tcpStartupStarted, undefined).pipe(
            Effect.andThen(
              Effect.sync(() => {
                events.push("acquire:tcpPublishIngress");
              }),
            ),
            Effect.andThen(Effect.never),
          ),
      };

      const startupFiber = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(tcpStartupStarted);
      yield* Fiber.interrupt(startupFiber);
      const startupExit = yield* Fiber.await(startupFiber);

      expect(Exit.hasInterrupts(startupExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "acquire:kafkaIngress",
        "acquire:grpcIngress",
        "acquire:tcpPublishIngress",
        "close:grpcIngress",
        "close:kafkaIngress",
        "close:server",
        "close:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("finishes Scope finalization when the first close caller is interrupted", () =>
    Effect.gen(function* () {
      const cleanupStarted = yield* Deferred.make<void>();
      const allowCleanup = yield* Deferred.make<void>();
      let serverCloseCount = 0;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            metricsUrl: "http://127.0.0.1:0/metrics",
            close: Deferred.succeed(cleanupStarted, undefined).pipe(
              Effect.andThen(Deferred.await(allowCleanup)),
              Effect.andThen(
                Effect.sync(() => {
                  serverCloseCount += 1;
                }),
              ),
            ),
          }),
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer);
      const closeFiber = yield* runtime.close.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cleanupStarted);
      const interruptFiber = yield* Fiber.interrupt(closeFiber).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;

      const interruptBeforeRelease = interruptFiber.pollUnsafe();
      yield* Deferred.succeed(allowCleanup, undefined);
      yield* Fiber.join(interruptFiber);
      expect(interruptBeforeRelease).toBeUndefined();
      yield* runtime.close;

      expect(serverCloseCount).toBe(1);
    }),
  );

  it.effect("run owns the same runtime Scope until its launch fiber is interrupted", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const runtimeStarted = yield* Deferred.make<void>();
      const tracked = makeTrackedAllSourceDependencies(events);
      const dependencies = {
        ...tracked,
        makeTcpPublishIngress: (...args: Parameters<typeof tracked.makeTcpPublishIngress>) =>
          tracked
            .makeTcpPublishIngress(...args)
            .pipe(Effect.tap(() => Deferred.succeed(runtimeStarted, undefined))),
      };
      const runtimeFiber = yield* runViewServerRuntimeWithDependencies(
        dependencies,
        allSourceViewServer,
        allSourceRuntimeOptions,
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(runtimeStarted);

      expect(runtimeFiber.pollUnsafe()).toBeUndefined();
      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "acquire:kafkaIngress",
        "acquire:grpcIngress",
        "acquire:tcpPublishIngress",
      ]);
      yield* Fiber.interrupt(runtimeFiber);

      expect(events).toStrictEqual([
        "acquire:runtimeCore",
        "acquire:grpcLeaseManager",
        "acquire:server",
        "acquire:kafkaIngress",
        "acquire:grpcIngress",
        "acquire:tcpPublishIngress",
        "close:tcpPublishIngress",
        "close:grpcIngress",
        "close:kafkaIngress",
        "close:server",
        "close:grpcLeaseManager",
        "close:runtimeCore",
      ]);
    }),
  );

  it.effect("closes the full runtime Scope when startup logging defects", () => {
    const events: Array<string> = [];
    const logger = Logger.make<unknown, void>(() => {
      throw new Error("runtime startup logger defect");
    });

    return runViewServerRuntimeWithDependencies(
      makeTrackedAllSourceDependencies(events),
      allSourceViewServer,
      allSourceRuntimeOptions,
    ).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.exit,
      Effect.tap((exit) =>
        Effect.sync(() => {
          expect(Exit.hasDies(exit)).toBe(true);
          expect(events).toStrictEqual([
            "acquire:runtimeCore",
            "acquire:grpcLeaseManager",
            "acquire:server",
            "acquire:kafkaIngress",
            "acquire:grpcIngress",
            "acquire:tcpPublishIngress",
            "close:tcpPublishIngress",
            "close:grpcIngress",
            "close:kafkaIngress",
            "close:server",
            "close:grpcLeaseManager",
            "close:runtimeCore",
          ]);
        }),
      ),
    );
  });
});
