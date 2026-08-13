import { create, toBinary, type Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { compressionGzip, Http2SessionManager } from "@connectrpc/connect-node";
import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { makeViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import {
  Config,
  ConfigProvider,
  Context,
  Effect,
  Exit,
  Layer,
  Logger,
  Option,
  References,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { Buffer } from "node:buffer";
import * as Http2 from "node:http2";
import * as Net from "node:net";
import { grpc, GrpcSourceAdapter } from "./model";
import { grpcNode, grpcNodeLayer, GrpcNodeConfigurationError } from "./node";
import { awaitTestCondition } from "./test-support";

type RequestMessage = Message<"grpc.node.Request"> & {
  readonly region: string;
};

type EventMessage = Message<"grpc.node.Event"> & {
  readonly id: string;
};

const descriptorFile = fileDesc(
  globalThis.btoa(
    Array.from(
      toBinary(
        FileDescriptorProtoSchema,
        create(FileDescriptorProtoSchema, {
          name: "grpc/node.proto",
          package: "grpc.node",
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
              ],
            },
          ],
          service: [
            {
              name: "Orders",
              method: [
                {
                  name: "Stream",
                  inputType: ".grpc.node.Request",
                  outputType: ".grpc.node.Event",
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
const OrdersService = serviceDesc<{
  readonly stream: {
    readonly input: typeof RequestSchema;
    readonly output: typeof EventSchema;
    readonly methodKind: "server_streaming";
  };
}>(descriptorFile, 0);

const Row = Schema.Struct({
  id: ViewServerId,
});

const source = grpc.topicSources({ orders: OrdersService }).materialized(
  {
    client: "orders",
    method: "stream",
    request: () => ({ region: "all" }),
    map: ({ value }) => ({ id: value.id }),
  },
  Schedule.recurs(0),
);

const config = defineViewServerConfig({
  topics: {
    orders: {
      schema: Row,
      source,
    },
  },
});

const invokeLayer = <Options>(options: Options): unknown =>
  Reflect.apply(grpcNodeLayer, undefined, [config, options]);

const isClosedLayer = (value: unknown): value is Layer.Layer<unknown, unknown, never> =>
  Layer.isLayer(value);

const awaitNodeRequest = (read: () => number): Effect.Effect<void> =>
  awaitTestCondition(
    () => `the stateful HTTP/2 resource request; last observed ${read()}`,
    () => read() === 1,
    10_000,
    Effect.yieldNow,
  );

const makeUnavailableHttp2Server = Effect.fn("GrpcSourceAdapter.test.node.server")(function* () {
  const server = yield* Effect.acquireRelease(
    Effect.callback<Http2.Http2Server>((resume) => {
      const created = Http2.createServer();
      created.on("stream", (stream: Http2.ServerHttp2Stream) => {
        const session = stream.session;
        stream.respond({ ":status": 503 });
        stream.end();
        session?.close();
      });
      created.once("error", (cause) => resume(Effect.die(cause)));
      created.listen(0, "127.0.0.1", () => resume(Effect.succeed(created)));
    }),
    (created) =>
      Effect.callback<void>((resume) => {
        created.close(() => resume(Effect.void));
      }),
  );
  const address = server.address();
  if (typeof address !== "object" || address === null || typeof address.port !== "number") {
    return yield* Effect.die("The stateful-resource test server did not expose a TCP port.");
  }
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
  };
});

describe("gRPC aggregate Node Layer", () => {
  it("rejects malformed resolved options before Layer acquisition", () => {
    let accessorCalls = 0;
    const accessorOptions = {};
    Object.defineProperty(accessorOptions, "orders", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return { baseUrl: "http://127.0.0.1" };
      },
    });
    const accessorEntry = {};
    Object.defineProperty(accessorEntry, "baseUrl", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return "http://127.0.0.1";
      },
    });
    const accessorTransport = {};
    Object.defineProperty(accessorTransport, "nodeOptions", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return {};
      },
    });
    const sparseInterceptors: Array<unknown> = [() => undefined];
    Reflect.deleteProperty(sparseInterceptors, "0");
    const accessorInterceptors: Array<unknown> = [];
    Object.defineProperty(accessorInterceptors, "0", {
      enumerable: true,
      get: () => {
        accessorCalls += 1;
        return () => undefined;
      },
    });
    Object.defineProperty(accessorInterceptors, "length", {
      value: 1,
    });
    const invalidLengthInterceptors = new Proxy([() => undefined], {
      getOwnPropertyDescriptor: (target, key) =>
        key === "length"
          ? {
              configurable: false,
              enumerable: false,
              value: 1.5,
              writable: true,
            }
          : Reflect.getOwnPropertyDescriptor(target, key),
    });
    const invalid: ReadonlyArray<unknown> = [
      null,
      [],
      new (class Options {})(),
      {},
      accessorOptions,
      { orders: accessorEntry },
      { [Symbol.for("orders")]: { baseUrl: "http://127.0.0.1" } },
      { "": { baseUrl: "http://127.0.0.1" } },
      { orders: null },
      { orders: {} },
      { orders: { baseUrl: "" } },
      { orders: { baseUrl: 1 } },
      { orders: { baseUrl: "http://127.0.0.1", extra: true } },
      { orders: { baseUrl: "http://127.0.0.1", interceptors: {} } },
      { orders: { baseUrl: "http://127.0.0.1", interceptors: [1] } },
      { orders: { baseUrl: "http://127.0.0.1", interceptors: sparseInterceptors } },
      { orders: { baseUrl: "http://127.0.0.1", interceptors: accessorInterceptors } },
      { orders: { baseUrl: "http://127.0.0.1", interceptors: invalidLengthInterceptors } },
      { orders: { baseUrl: "http://127.0.0.1", transport: null } },
      { orders: { baseUrl: "http://127.0.0.1", transport: "invalid" } },
      { orders: { baseUrl: "http://127.0.0.1", transport: accessorTransport } },
      {
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: { unknownTransportOption: true },
        },
      },
      {
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: { useBinaryFormat: "yes" },
        },
      },
      {
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: { compressMinBytes: -1 },
        },
      },
      {
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: { acceptCompression: [1] },
        },
      },
      {
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: { sendCompression: null },
        },
      },
      {
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: { binaryOptions: [] },
        },
      },
      new Proxy(
        {},
        {
          ownKeys: () => {
            throw new Error("proxy traps must become named configuration failures");
          },
        },
      ),
    ];
    for (const options of invalid) {
      expect(() => invokeLayer(options)).toThrow(GrpcNodeConfigurationError);
    }
    const accessorNodeOptions = {};
    Object.defineProperty(accessorNodeOptions, "settings", {
      enumerable: true,
      get: () => {
        throw new Error("nested transport accessors must not execute");
      },
    });
    const symbolNodeOptions = {
      [Symbol("hidden")]: true,
    };
    const nonEnumerableNodeOptions = {};
    Object.defineProperty(nonEnumerableNodeOptions, "hidden", {
      value: true,
    });
    const missingDescriptorNodeOptions = new Proxy(
      {},
      {
        ownKeys: () => ["ghost"],
        getOwnPropertyDescriptor: () => undefined,
      },
    );
    let changingNodeOptionReads = 0;
    const changingNodeOptions = new Proxy(
      {},
      {
        ownKeys: () => {
          changingNodeOptionReads += 1;
          return changingNodeOptionReads === 1 ? [] : ["ghost"];
        },
        getOwnPropertyDescriptor: () => undefined,
      },
    );
    const sparseNestedArray = [1];
    Reflect.deleteProperty(sparseNestedArray, "0");
    for (const nodeOptions of [
      accessorNodeOptions,
      symbolNodeOptions,
      nonEnumerableNodeOptions,
      missingDescriptorNodeOptions,
      changingNodeOptions,
      { sparseNestedArray },
    ]) {
      expect(() =>
        invokeLayer({
          orders: {
            baseUrl: "http://127.0.0.1",
            transport: {
              nodeOptions,
            },
          },
        }),
      ).toThrow(GrpcNodeConfigurationError);
    }
    expect(accessorCalls).toBe(0);
    const compression = {
      name: "test",
      compress: (bytes: Uint8Array) => Promise.resolve(bytes),
      decompress: (bytes: Uint8Array) => Promise.resolve(bytes),
    };
    const sessionManager = {
      authority: "http://127.0.0.1",
      request: () => Promise.reject(new Error("not invoked by Layer acquisition")),
      notifyResponseByteRead: () => undefined,
    };
    expect(
      isClosedLayer(
        invokeLayer({
          orders: {
            baseUrl: "http://127.0.0.1",
            transport: {
              acceptCompression: [compression],
              binaryOptions: {},
              compressMinBytes: 0,
              defaultTimeoutMs: 0,
              idleConnectionTimeoutMs: 0,
              jsonOptions: {},
              nodeOptions: {},
              pingIdleConnection: false,
              pingIntervalMs: 0,
              pingTimeoutMs: 0,
              readMaxBytes: 0,
              sendCompression: compression,
              sessionManager,
              useBinaryFormat: true,
              writeMaxBytes: 0,
            },
          },
        }),
      ),
    ).toBe(true);
  });

  it.effect("fails Layer acquisition for malformed compression and session-manager resources", () =>
    Effect.gen(function* () {
      const transform = (bytes: Uint8Array) => Promise.resolve(bytes);
      const validCompression = {
        name: "test",
        compress: transform,
        decompress: transform,
      };
      const validSessionManager = {
        authority: "http://127.0.0.1",
        request: () => Promise.reject(new Error("not invoked by Layer acquisition")),
        notifyResponseByteRead: () => undefined,
      };
      const validLayer = invokeLayer({
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: {
            acceptCompression: [validCompression],
            sendCompression: validCompression,
            sessionManager: validSessionManager,
          },
        },
      });
      if (!isClosedLayer(validLayer)) {
        return yield* Effect.die("Expected reflected grpcNode.layer result.");
      }
      const validExit = yield* Effect.scoped(Layer.build(validLayer)).pipe(Effect.exit);
      expect(Exit.isSuccess(validExit)).toBe(true);

      let accessorCalls = 0;
      const accessorCompression = new (class Compression {
        get name(): string {
          accessorCalls += 1;
          return "test";
        }

        readonly compress = transform;
        readonly decompress = transform;
      })();
      const accessorSessionManager = new (class SessionManager {
        get authority(): string {
          accessorCalls += 1;
          return "http://127.0.0.1";
        }

        get request(): () => Promise<never> {
          accessorCalls += 1;
          return () => Promise.reject(new Error("not invoked by Layer acquisition"));
        }

        get notifyResponseByteRead(): () => void {
          accessorCalls += 1;
          return () => undefined;
        }
      })();
      const accessorLayer = invokeLayer({
        orders: {
          baseUrl: "http://127.0.0.1",
          transport: {
            acceptCompression: [accessorCompression],
            sendCompression: accessorCompression,
            sessionManager: accessorSessionManager,
          },
        },
      });
      if (!isClosedLayer(accessorLayer)) {
        return yield* Effect.die("Expected accessor-backed grpcNode.layer result.");
      }
      const accessorExit = yield* Effect.scoped(Layer.build(accessorLayer)).pipe(Effect.exit);
      expect(Exit.isSuccess(accessorExit)).toBe(true);
      expect(accessorCalls).toBeGreaterThan(0);

      const throwingAccessorCompression = new (class Compression {
        get name(): string {
          throw new Error("planned compression accessor failure");
        }

        readonly compress = transform;
        readonly decompress = transform;
      })();
      const throwingAccessorSessionManager = new (class SessionManager {
        get authority(): string {
          throw new Error("planned session manager accessor failure");
        }

        request(): Promise<never> {
          return new Promise<never>(() => undefined);
        }

        notifyResponseByteRead(): void {}
      })();
      let prototypeReads = 0;
      const hostileCompression = new Proxy(new (class Compression {})(), {
        getPrototypeOf: (target) => {
          prototypeReads += 1;
          if (prototypeReads > 2) {
            throw new Error("planned compression prototype failure");
          }
          return Reflect.getPrototypeOf(target);
        },
      });
      const malformedTransports = [
        {
          acceptCompression: [{}],
        },
        {
          acceptCompression: [
            {
              name: "",
              compress: transform,
              decompress: transform,
            },
          ],
        },
        {
          acceptCompression: [
            {
              name: "test",
              compress: null,
              decompress: transform,
            },
          ],
        },
        {
          acceptCompression: [
            {
              name: "test",
              compress: transform,
              decompress: null,
            },
          ],
        },
        {
          acceptCompression: [throwingAccessorCompression],
        },
        {
          acceptCompression: [hostileCompression],
        },
        {
          sendCompression: {},
        },
        {
          sessionManager: {},
        },
        {
          sessionManager: throwingAccessorSessionManager,
        },
      ];
      for (const transport of malformedTransports) {
        const reflected = invokeLayer({
          orders: {
            baseUrl: "http://127.0.0.1",
            transport,
          },
        });
        if (!isClosedLayer(reflected)) {
          return yield* Effect.die("Expected reflected grpcNode.layer result.");
        }
        const exit = yield* Effect.scoped(Layer.build(reflected)).pipe(Effect.exit);
        expect(Exit.findErrorOption(exit)).toStrictEqual(
          Option.some({
            _tag: "GrpcConfigurationFailure",
            client: "orders",
            message: "Logical gRPC client orders contains malformed transport resources.",
            phase: "client-construction",
          }),
        );
      }
    }),
  );

  it.effect("builds frozen resolved/config options and preserves Config errors", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directContext = yield* Layer.build(
          grpcNode.layer(config, {
            orders: {
              baseUrl: "http://127.0.0.1",
              interceptors: [],
              transport: {},
            },
          }),
        );
        const provider = ConfigProvider.fromUnknown({
          ORDERS_URL: "http://127.0.0.1",
        });
        const configuredContext = yield* Layer.build(
          grpcNode.layerConfig(config, {
            orders: {
              baseUrl: Config.string("ORDERS_URL"),
            },
          }),
        ).pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider));
        const wholeConfiguredContext = yield* Layer.build(
          grpcNode.layerConfig(
            config,
            Config.succeed({
              orders: {
                baseUrl: "http://127.0.0.1",
              },
            }),
          ),
        );
        const missingConfigExit = yield* Layer.build(
          grpcNode.layerConfig(config, {
            orders: {
              baseUrl: Config.string("MISSING_ORDERS_URL"),
            },
          }),
        ).pipe(Effect.provideService(ConfigProvider.ConfigProvider, provider), Effect.exit);
        const invalidResolvedConfigExit = yield* Layer.build(
          grpcNode.layerConfig(config, {
            orders: {
              baseUrl: Config.succeed(""),
            },
          }),
        ).pipe(Effect.exit);

        expect({
          directService: Context.getOption(directContext, source.adapter.runtimeService)._tag,
          configuredService: Context.getOption(configuredContext, source.adapter.runtimeService)
            ._tag,
          wholeConfiguredService: Context.getOption(
            wholeConfiguredContext,
            source.adapter.runtimeService,
          )._tag,
          missingConfigFailure: Exit.isFailure(missingConfigExit),
          invalidResolvedConfigFailure: Exit.findErrorOption(invalidResolvedConfigExit),
        }).toStrictEqual({
          directService: "Some",
          configuredService: "Some",
          wholeConfiguredService: "Some",
          missingConfigFailure: true,
          invalidResolvedConfigFailure: Option.some({
            _tag: "GrpcConfigurationFailure",
            client: "",
            message:
              "The resolved gRPC Node configuration must contain exact non-empty client options.",
            phase: "client-construction",
          }),
        });
      }),
    ),
  );

  it.effect("rejects hostile outer Config wrappers without evaluating their contents", () =>
    Effect.gen(function* () {
      let accessorCalls = 0;
      const accessor = {};
      Object.defineProperty(accessor, "orders", {
        enumerable: true,
        get: () => {
          accessorCalls += 1;
          return { baseUrl: "http://127.0.0.1" };
        },
      });
      const withSymbol = {
        orders: {
          baseUrl: "http://127.0.0.1",
        },
        [Symbol("hidden")]: true,
      };
      const withNonEnumerable = {
        orders: {
          baseUrl: "http://127.0.0.1",
        },
      };
      Object.defineProperty(withNonEnumerable, "hidden", {
        value: true,
      });
      const proxy = new Proxy(
        {},
        {
          ownKeys: () => {
            throw new Error("outer Config reflection failure");
          },
        },
      );

      for (const options of [accessor, withSymbol, withNonEnumerable, proxy, { orders: 1 }]) {
        const reflected = Reflect.apply(grpcNode.layerConfig, undefined, [config, options]);
        if (!isClosedLayer(reflected)) {
          return yield* Effect.die("Expected reflected grpcNode.layerConfig result.");
        }
        const exit = yield* Effect.scoped(Layer.build(reflected)).pipe(Effect.exit);
        expect(Exit.findErrorOption(exit)).toStrictEqual(
          Option.some({
            _tag: "GrpcConfigurationFailure",
            client: "",
            message: "The gRPC Node Config input must contain exact enumerable string data fields.",
            phase: "client-construction",
          }),
        );
      }
      expect(accessorCalls).toBe(0);
    }),
  );

  it.effect("does not finalize a caller-owned HTTP/2 session manager", () =>
    Effect.gen(function* () {
      const manager = new Http2SessionManager("http://127.0.0.1");
      const abort = manager.abort.bind(manager);
      let abortCalls = 0;
      manager.abort = () => {
        abortCalls += 1;
      };

      yield* Effect.scoped(
        Layer.build(
          grpcNode.layer(config, {
            orders: {
              baseUrl: "http://127.0.0.1",
              transport: {
                sessionManager: manager,
              },
            },
          }),
        ),
      );

      expect(abortCalls).toBe(0);
      abort();
    }),
  );

  it.effect("contains defects raised while finalizing an owned HTTP/2 session manager", () => {
    const logs: Array<{
      readonly cause: string;
      readonly message: unknown;
    }> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push({
        cause: String(options.cause),
        message: options.message,
      });
    });
    return Effect.gen(function* () {
      const abortDescriptor = Option.getOrThrow(
        Option.fromUndefinedOr(
          Object.getOwnPropertyDescriptor(Http2SessionManager.prototype, "abort"),
        ),
      );
      let abortCalls = 0;
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              Object.defineProperty(Http2SessionManager.prototype, "abort", {
                configurable: true,
                value: () => {
                  abortCalls += 1;
                  throw new Error("private-session-finalization-sentinel");
                },
              });
            }),
            () =>
              Effect.sync(() => {
                Object.defineProperty(Http2SessionManager.prototype, "abort", abortDescriptor);
              }),
          );
          yield* Layer.build(
            grpcNode.layer(config, {
              orders: {
                baseUrl: "http://127.0.0.1",
              },
            }),
          );
        }),
      );

      expect({
        abortCalls,
        causeContainsSentinel: logs.some((entry) =>
          entry.cause.includes("private-session-finalization-sentinel"),
        ),
        messages: logs.map((entry) => entry.message),
      }).toStrictEqual({
        abortCalls: 1,
        causeContainsSentinel: false,
        messages: [["gRPC Node HTTP/2 session manager finalization failed."]],
      });
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("preserves stateful caller-owned HTTP/2 resources for layer and layerConfig", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const mode of ["resolved", "config"] as const) {
          const testServer = yield* makeUnavailableHttp2Server();
          const receivers: Array<unknown> = [];
          let resourceReads = 0;
          const delegate = new Http2SessionManager(testServer.baseUrl);
          const managerTarget = {
            authority: testServer.baseUrl,
            calls: 0,
            request(...arguments_: Parameters<Http2SessionManager["request"]>) {
              receivers.push(this);
              this.calls += 1;
              return delegate.request(...arguments_);
            },
            notifyResponseByteRead(
              ...arguments_: Parameters<Http2SessionManager["notifyResponseByteRead"]>
            ) {
              return delegate.notifyResponseByteRead(...arguments_);
            },
          };
          const manager = new Proxy(managerTarget, {
            get: (target, key, receiver) => {
              resourceReads += 1;
              return Reflect.get(target, key, receiver);
            },
          });
          const options = {
            orders: {
              baseUrl: testServer.baseUrl,
              transport: {
                sessionManager: manager,
              },
            },
          };
          const layer =
            mode === "resolved"
              ? grpcNode.layer(config, options)
              : grpcNode.layerConfig(config, Config.succeed(options));
          const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(Effect.provide(layer));
          yield* awaitNodeRequest(() => managerTarget.calls);
          const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({ topic: "orders" });
          const exhausted = Option.getOrThrow(
            yield* diagnostics.events.pipe(
              Stream.filter((health) => health.status._tag === "Exhausted"),
              Stream.take(1),
              Stream.runHead,
            ),
          );
          expect({
            calls: managerTarget.calls,
            receiverIsCallerResource: receivers[0] === manager,
            status: exhausted.status._tag,
          }).toStrictEqual({
            calls: 1,
            receiverIsCallerResource: true,
            status: "Exhausted",
          });
          expect(resourceReads).toBeGreaterThan(3);
          yield* diagnostics.close();
          yield* runtime.close;
          delegate.abort();
        }
      }),
    ),
  );

  it.effect("preserves cloned Buffer TLS options through real layer connections", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const abortDescriptor = Option.getOrThrow(
          Option.fromUndefinedOr(
            Object.getOwnPropertyDescriptor(Http2SessionManager.prototype, "abort"),
          ),
        );
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            Object.defineProperty(Http2SessionManager.prototype, "abort", {
              configurable: true,
              value: () => undefined,
            });
          }),
          () =>
            Effect.sync(() => {
              Object.defineProperty(Http2SessionManager.prototype, "abort", abortDescriptor);
            }),
        );
        for (const mode of ["resolved", "config"] as const) {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const testServer = yield* makeUnavailableHttp2Server();
              const ca = Buffer.from([1, 2]);
              const pfx = Buffer.from([3, 4]);
              const observations: Array<{
                readonly caBytes: ReadonlyArray<number>;
                readonly caIsBuffer: boolean;
                readonly caIsClone: boolean;
                readonly pfxBytes: ReadonlyArray<number>;
                readonly pfxIsBuffer: boolean;
                readonly pfxIsClone: boolean;
              }> = [];
              const options = {
                orders: {
                  baseUrl: testServer.baseUrl,
                  transport: {
                    nodeOptions: {
                      ca,
                      pfx: [pfx],
                      createConnection: (
                        _authority: URL,
                        connectionOptions: Http2.SessionOptions,
                      ) => {
                        const capturedCa = Reflect.get(connectionOptions, "ca");
                        const capturedPfx = Reflect.get(connectionOptions, "pfx");
                        const capturedPfxEntry = Array.isArray(capturedPfx)
                          ? capturedPfx[0]
                          : undefined;
                        observations.push({
                          caBytes: Buffer.isBuffer(capturedCa) ? [...capturedCa] : [],
                          caIsBuffer: Buffer.isBuffer(capturedCa),
                          caIsClone: capturedCa !== ca,
                          pfxBytes: Buffer.isBuffer(capturedPfxEntry) ? [...capturedPfxEntry] : [],
                          pfxIsBuffer: Buffer.isBuffer(capturedPfxEntry),
                          pfxIsClone: capturedPfxEntry !== pfx,
                        });
                        return Net.connect(Number(new URL(testServer.baseUrl).port), "127.0.0.1");
                      },
                    },
                  },
                },
              };
              const layer =
                mode === "resolved"
                  ? grpcNode.layer(config, options)
                  : grpcNode.layerConfig(config, Config.succeed(options));
              const runtime = yield* makeViewServerRuntimeCore(config, {}).pipe(
                Effect.provide(layer),
              );
              yield* awaitNodeRequest(() => observations.length);
              const diagnostics = yield* runtime.liveClient.subscribeSourceHealth({
                topic: "orders",
              });
              const exhausted = Option.getOrThrow(
                yield* diagnostics.events.pipe(
                  Stream.filter((health) => health.status._tag === "Exhausted"),
                  Stream.take(1),
                  Stream.runHead,
                ),
              );

              expect({
                observations,
                status: exhausted.status._tag,
              }).toStrictEqual({
                observations: [
                  {
                    caBytes: [1, 2],
                    caIsBuffer: true,
                    caIsClone: true,
                    pfxBytes: [3, 4],
                    pfxIsBuffer: true,
                    pfxIsClone: true,
                  },
                ],
                status: "Exhausted",
              });
              yield* diagnostics.close();
              yield* runtime.close;
            }),
          );
        }
      }),
    ),
  );

  it.effect("constructs and scopes every referenced logical client for layer and layerConfig", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const multiSources = grpc.topicSources({
          orders: OrdersService,
          strategies: OrdersService,
        });
        const ordersSource = multiSources.materialized({
          client: "orders",
          method: "stream",
          request: () => ({ region: "orders" }),
          map: ({ value }) => ({ id: value.id }),
        });
        const strategiesSource = multiSources.materialized({
          client: "strategies",
          method: "stream",
          request: () => ({ region: "strategies" }),
          map: ({ value }) => ({ id: value.id }),
        });
        const multiConfig = defineViewServerConfig({
          topics: {
            orders: {
              schema: Row,
              source: ordersSource,
            },
            strategies: {
              schema: Row,
              source: strategiesSource,
            },
          },
        });
        const abortDescriptor = Option.getOrThrow(
          Option.fromUndefinedOr(
            Object.getOwnPropertyDescriptor(Http2SessionManager.prototype, "abort"),
          ),
        );
        const closedAuthorities: Array<string> = [];
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            Object.defineProperty(Http2SessionManager.prototype, "abort", {
              configurable: true,
              value(this: Http2SessionManager) {
                closedAuthorities.push(this.authority);
              },
            });
          }),
          () =>
            Effect.sync(() => {
              Object.defineProperty(Http2SessionManager.prototype, "abort", abortDescriptor);
            }),
        );
        const options = {
          orders: {
            baseUrl: "http://orders.example",
          },
          strategies: {
            baseUrl: "http://strategies.example",
          },
        };
        const direct = yield* Effect.scoped(Layer.build(grpcNode.layer(multiConfig, options)));
        const configured = yield* Effect.scoped(
          Layer.build(grpcNode.layerConfig(multiConfig, Config.succeed(options))),
        );
        const directReporting = Option.getOrThrow(
          Option.fromNullishOr(
            Context.getUnsafe(direct, GrpcSourceAdapter.runtimeService).reporting,
          ),
        );
        const configuredReporting = Option.getOrThrow(
          Option.fromNullishOr(
            Context.getUnsafe(configured, GrpcSourceAdapter.runtimeService).reporting,
          ),
        );

        expect({
          closedAuthorities: closedAuthorities.toSorted(),
          configuredService: Context.getOption(configured, GrpcSourceAdapter.runtimeService)._tag,
          configuredTargets: yield* configuredReporting.dependencies({
            topic: "orders",
            lifecycle: "materialized",
            definition: ordersSource.options,
          }),
          directService: Context.getOption(direct, GrpcSourceAdapter.runtimeService)._tag,
          directTargets: yield* directReporting.dependencies({
            topic: "strategies",
            lifecycle: "materialized",
            definition: strategiesSource.options,
          }),
        }).toStrictEqual({
          closedAuthorities: [
            "http://orders.example",
            "http://orders.example",
            "http://strategies.example",
            "http://strategies.example",
          ],
          configuredService: "Some",
          configuredTargets: [
            {
              target: "orders",
              endpoints: ["http://orders.example"],
            },
          ],
          directService: "Some",
          directTargets: [
            {
              target: "strategies",
              endpoints: ["http://strategies.example"],
            },
          ],
        });
      }),
    ),
  );

  it.effect("retains __proto__ logical clients through aggregate Layer assembly", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const protoDescriptors = {};
        Object.defineProperty(protoDescriptors, "__proto__", {
          enumerable: true,
          value: OrdersService,
        });
        const protoSources = Reflect.apply(grpc.topicSources, grpc, [protoDescriptors]);
        const protoSource = Reflect.apply(Reflect.get(protoSources, "materialized"), protoSources, [
          {
            client: "__proto__",
            method: "stream",
            request: () => ({ region: "all" }),
            map: () => ({ id: "one" }),
          },
        ]);
        const protoViewServer = Reflect.apply(defineViewServerConfig, undefined, [
          {
            topics: {
              orders: {
                schema: Row,
                source: protoSource,
              },
            },
          },
        ]);
        const protoOptions = {};
        Object.defineProperty(protoOptions, "__proto__", {
          enumerable: true,
          value: {
            baseUrl: "http://127.0.0.1",
          },
        });
        const layer = Reflect.apply(grpcNodeLayer, undefined, [protoViewServer, protoOptions]);
        if (!isClosedLayer(layer)) {
          return yield* Effect.die("Expected __proto__ aggregate gRPC Layer.");
        }

        const context = yield* Layer.build(layer);
        expect(Context.getOption(context, source.adapter.runtimeService)._tag).toBe("Some");
      }),
    ),
  );

  it.effect("fails aggregate acquisition for missing and extra logical client bindings", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const extraLayer = invokeLayer({
          orders: { baseUrl: "http://127.0.0.1" },
          extra: { baseUrl: "http://127.0.0.1" },
        });
        if (!isClosedLayer(extraLayer)) {
          return yield* Effect.die("Expected reflected grpcNode.layer result.");
        }
        const extraExit = yield* Layer.build(extraLayer).pipe(Effect.exit);
        const missingViewServer = defineViewServerConfig({
          topics: {
            orders: {
              schema: Row,
              source,
            },
            ordersAgain: {
              schema: Row,
              source: grpc
                .topicSources({
                  inventory: OrdersService,
                })
                .materialized({
                  client: "inventory",
                  method: "stream",
                  request: () => ({ region: "all" }),
                  map: ({ value }) => ({ id: value.id }),
                }),
            },
          },
        });
        const missingLayer = Reflect.apply(grpcNodeLayer, undefined, [
          missingViewServer,
          {
            orders: { baseUrl: "http://127.0.0.1" },
          },
        ]);
        if (!isClosedLayer(missingLayer)) {
          return yield* Effect.die("Expected reflected grpcNode.layer result.");
        }
        const missingExit = yield* Layer.build(missingLayer).pipe(Effect.exit);
        const invalidUrlExit = yield* Layer.build(
          grpcNode.layer(config, {
            orders: {
              baseUrl: "not a URL",
            },
          }),
        ).pipe(Effect.exit);
        const invalidTransportExit = yield* Layer.build(
          grpcNode.layer(config, {
            orders: {
              baseUrl: "http://127.0.0.1",
              transport: {
                readMaxBytes: 0,
              },
            },
          }),
        ).pipe(Effect.exit);

        expect({
          extra: Exit.isFailure(extraExit),
          missing: Exit.isFailure(missingExit),
          invalidTransport: Exit.isFailure(invalidTransportExit),
          invalidUrl: Exit.isFailure(invalidUrlExit),
        }).toStrictEqual({
          extra: true,
          missing: true,
          invalidTransport: true,
          invalidUrl: true,
        });
      }),
    ),
  );

  it.effect("snapshots nested transport options before Layer acquisition", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const settings = {
          enablePush: false,
        };
        const bytes = new Uint8Array([1, 2]);
        const priorities = [1, 2];
        const opaque = /opaque/;
        const acceptCompression: Array<typeof compressionGzip> = [];
        const jsonOptions = {
          ignoreUnknownFields: true,
        };
        const binaryOptions = {
          readUnknownFields: true,
        };
        const options = {
          orders: {
            baseUrl: "http://127.0.0.1",
            transport: {
              nodeOptions: {
                settings,
                bytes,
                priorities,
                opaque,
              },
              jsonOptions,
              binaryOptions,
              acceptCompression,
              idleConnectionTimeoutMs: 1_000,
              pingIdleConnection: true,
              pingIntervalMs: 500,
              pingTimeoutMs: 250,
            },
          },
        };
        const layer = grpcNode.layer(config, options);

        Reflect.set(options.orders, "baseUrl", "");
        Object.defineProperty(settings, "enablePush", {
          enumerable: true,
          get: () => {
            throw new Error("the captured settings snapshot must be isolated");
          },
        });
        Reflect.set(jsonOptions, "ignoreUnknownFields", false);
        Reflect.set(binaryOptions, "readUnknownFields", false);
        bytes[0] = 9;
        priorities[0] = 9;
        acceptCompression.push(compressionGzip);

        const context = yield* Layer.build(layer);
        expect(Context.getOption(context, source.adapter.runtimeService)._tag).toBe("Some");
      }),
    ),
  );
});
