import { NodeSocket } from "@effect/platform-node";
import {
  defineViewServerConfig,
  type GrpcRuntimeClients,
  type RuntimeRegions,
  type TopicDefinitions,
  type ViewServerConfig,
  type ViewServerHealth,
} from "@effect-view-server/config";
import { ViewServerRpcs } from "@effect-view-server/protocol";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import {
  Context,
  Effect,
  Layer,
  ManagedRuntime,
  Schema,
  SchemaGetter,
  Stream,
  SubscriptionRef,
} from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import * as Net from "node:net";
import {
  ViewServerAuthError,
  type ViewServerAuth,
  type ViewServerWebSocketServerInput,
} from "../src/index";

export const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

export const Trade = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigInt,
});

export const Quote = Schema.Struct({
  id: Schema.String,
  price: Schema.BigDecimal,
});

export const HealthJson = Schema.Struct({
  status: Schema.String,
  engine: Schema.Struct({
    topics: Schema.Struct({
      orders: Schema.Struct({
        rowCount: Schema.Number,
      }),
    }),
  }),
});

export const TcpAddress = Schema.Struct({
  port: Schema.Number,
});

export class ServerTestJsonParseError extends Schema.TaggedErrorClass<ServerTestJsonParseError>()(
  "ServerTestJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

export class ServerTestMalformedUpgradeError extends Schema.TaggedErrorClass<ServerTestMalformedUpgradeError>()(
  "ServerTestMalformedUpgradeError",
  {
    cause: Schema.Unknown,
  },
) {}

export class ServerTestTcpError extends Schema.TaggedErrorClass<ServerTestTcpError>()(
  "ServerTestTcpError",
  {
    cause: Schema.Unknown,
  },
) {}

export class ServerTestWebSocketOpenError extends Schema.TaggedErrorClass<ServerTestWebSocketOpenError>()(
  "ServerTestWebSocketOpenError",
  {
    cause: Schema.Unknown,
  },
) {}

export const BadJsonField = Schema.String.pipe(
  Schema.encodeTo(Schema.Any, {
    decode: SchemaGetter.transform((value) => (typeof value === "string" ? value : "decoded")),
    encode: SchemaGetter.transform(() => Symbol("not-json")),
  }),
);

export const BadJsonRow = Schema.Struct({
  id: Schema.String,
});

export const BadJsonRowEdge = Schema.Struct({
  id: BadJsonField,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
    quotes: {
      schema: Quote,
      key: "id",
    },
  },
});

export const safeEdgeViewServer = defineViewServerConfig({
  topics: {
    badjson: {
      schema: BadJsonRow,
      key: "id",
    },
  },
});
export const edgeViewServer = {
  ...safeEdgeViewServer,
  topics: {
    badjson: {
      ...safeEdgeViewServer.topics.badjson,
      schema: BadJsonRowEdge,
    },
  },
};

export const createServerTestRuntime = <
  const Topics extends TopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: Parameters<typeof makeViewServerRuntimeCoreInternal<Topics>>[1] = {},
) => {
  const runtimeCore = Effect.runSync(makeViewServerRuntimeCoreInternal(config, options));
  return {
    ...runtimeCore,
    liveClient: runtimeCore.serverLiveClient,
  };
};

export const serverTestLiveClientWithSubscribe = <const Topics extends TopicDefinitions>(
  base: Pick<
    ViewServerWebSocketServerInput<Topics>["liveClient"],
    "subscribeHealth" | "subscribeHealthSummary"
  >,
  subscribe: ViewServerWebSocketServerInput<Topics>["liveClient"]["subscribeProtocolQuery"],
): ViewServerWebSocketServerInput<Topics>["liveClient"] => ({
  subscribeHealth: base.subscribeHealth,
  subscribeHealthSummary: base.subscribeHealthSummary,
  subscribeProtocolSourceHealth: (topic) =>
    Effect.fail({
      _tag: "ViewServerRuntimeError",
      code: "InvalidQuery",
      message: `Topic ${topic} has no Source.`,
      topic,
    }),
  subscribeProtocolQuery: subscribe,
});

export const kafkaStartFromHealth = {
  consumerGroupId: "view-server-test",
  fallbackMode: "latest",
  mode: "latest",
} as const;

export const bearerAuth: ViewServerAuth = {
  validateRequest: (request) =>
    request.headers["authorization"] === "Bearer view-server-test"
      ? Effect.succeed({
          forwardedHeaders: {
            authorization: request.headers["authorization"],
          },
          id: "session-1",
          systemHeaders: {},
        })
      : Effect.fail(
          new ViewServerAuthError({
            message: "Missing or invalid authorization header.",
            status: 401,
          }),
        ),
};

export type OrderRow = typeof Order.Type;
export type TradeRow = typeof Trade.Type;
export type QuoteRow = typeof Quote.Type;

export const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

export const trade = (id: string, quantity: bigint): TradeRow => ({
  id,
  quantity,
});

export const quote = (id: string, price: string): QuoteRow => ({
  id,
  price: fromStringUnsafe(price),
});

export const serverHealthWithOrdersRowCount = (
  health: ViewServerHealth<typeof viewServer.topics>,
  rowCount: number,
): ViewServerHealth<typeof viewServer.topics> => ({
  ...health,
  engine: {
    ...health.engine,
    topics: {
      ...health.engine.topics,
      orders: {
        ...health.engine.topics.orders,
        rowCount,
      },
    },
  },
});

export const degradedServerHealth = (
  baseHealth: ViewServerHealth<typeof viewServer.topics>,
): ViewServerHealth<typeof viewServer.topics> => ({
  ...baseHealth,
  status: "degraded",
  kafka: {
    startFrom: kafkaStartFromHealth,
    regions: {},
    topics: {
      source_orders: {
        status: "degraded",
        sourceTopic: "source_orders",
        viewServerTopic: "orders",
        regions: {
          usa: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 0,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            commitFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: null,
            lastCommitAt: null,
            consumerLagMessages: 42n,
            lagSampledAt: null,
            committedOffset: null,
            lastError: null,
          },
          london: {
            connected: false,
            assignedPartitions: 0,
            messagesPerSecond: 7,
            bytesPerSecond: 70,
            decodedMessagesPerSecond: 6,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 2,
            publishFailuresPerSecond: 3,
            commitFailuresPerSecond: 4,
            processingFailuresPerSecond: 5,
            lastMessageAt: null,
            lastCommitAt: null,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: "11",
            lastError: "disconnected",
          },
        },
      },
    },
  },
  grpc: {
    clients: {
      ordersClient: {
        status: "connected",
        baseUrl: "http://127.0.0.1:8080",
        activeFeeds: 3,
        lastConnectedAt: null,
        lastError: null,
      },
    },
    feeds: {
      orders: {
        materialized: {
          ordersFeed: {
            status: "ready",
            lifecycle: "materialized",
            feedName: "ordersFeed",
            feedKey: "ordersFeed",
            topic: "orders",
            subscriberCount: 2,
            rowCount: 5,
            messagesPerSecond: 9,
            rowsPerSecond: 8,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            reconnects: 0,
            lastMessageAt: null,
            lastError: null,
          },
        },
        leased: {
          "ordersLease:strategy=strat-1": {
            status: "ready",
            lifecycle: "leased",
            feedName: "ordersLease",
            feedKey: "ordersLease:strategy=strat-1",
            topic: "orders",
            subscriberCount: 1,
            rowCount: 3,
            messagesPerSecond: 4,
            rowsPerSecond: 3,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            publishFailuresPerSecond: 0,
            reconnects: 0,
            lastMessageAt: null,
            lastError: null,
          },
          "ordersLease:strategy=strat-2": {
            status: "ready",
            lifecycle: "leased",
            feedName: "ordersLease",
            feedKey: "ordersLease:strategy=strat-2",
            topic: "orders",
            subscriberCount: 2,
            rowCount: 7,
            messagesPerSecond: 6,
            rowsPerSecond: 5,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 2,
            publishFailuresPerSecond: 3,
            reconnects: 4,
            lastMessageAt: null,
            lastError: null,
          },
        },
      },
    },
  },
});

export class RawViewServerRpcClient extends Context.Service<
  RawViewServerRpcClient,
  RpcClient.FromGroup<typeof ViewServerRpcs, RpcClientError>
>()("RawViewServerRpcClient") {}

export const makeRawRpcClient = Effect.fn("ViewServerServer.test.rawRpcClient.make")(function* (
  url: string,
) {
  const layer: Layer.Layer<RawViewServerRpcClient, never, never> = Layer.effect(
    RawViewServerRpcClient,
  )(RpcClient.make(ViewServerRpcs)).pipe(
    Layer.provide(RpcClient.layerProtocolSocket()),
    Layer.provide([NodeSocket.layerWebSocket(url), RpcSerialization.layerNdjson]),
  );
  const runtime = ManagedRuntime.make(layer);
  const context = yield* runtime.contextEffect;
  return {
    close: runtime.disposeEffect,
    rpc: Context.get(context, RawViewServerRpcClient),
  };
});

export const fetchJson = Effect.fn("ViewServerServer.test.fetchJson")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new ServerTestJsonParseError({ cause }),
  });
  return { response, value };
});

export const fetchText = Effect.fn("ViewServerServer.test.fetchText")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  return { response, text };
});

export const fetchJsonWithAuthorization = Effect.fn(
  "ViewServerServer.test.fetchJsonWithAuthorization",
)(function* (url: string, authorization: string) {
  const response = yield* Effect.promise(() =>
    fetch(url, {
      headers: {
        authorization,
      },
    }),
  );
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new ServerTestJsonParseError({ cause }),
  });
  return { response, value };
});

export const fetchTextWithAuthorization = Effect.fn(
  "ViewServerServer.test.fetchTextWithAuthorization",
)(function* (url: string, authorization: string) {
  const response = yield* Effect.promise(() =>
    fetch(url, {
      headers: {
        authorization,
      },
    }),
  );
  const text = yield* Effect.promise(() => response.text());
  return { response, text };
});

export const reserveTcpPort = Effect.fn("ViewServerServer.test.tcp.reservePort")(function* () {
  const server = yield* Effect.acquireRelease(
    Effect.callback<Net.Server, ServerTestTcpError>((resume, _signal) => {
      const blocker = Net.createServer();
      let reservationComplete = false;
      blocker.on("error", (cause) => {
        if (reservationComplete) {
          return;
        }
        reservationComplete = true;
        resume(Effect.fail(new ServerTestTcpError({ cause })));
      });
      blocker.listen(0, "127.0.0.1", () => {
        reservationComplete = true;
        resume(Effect.succeed(blocker));
      });
      return Effect.callback<void>((cleanupResume) => {
        blocker.close(() => {
          blocker.removeAllListeners();
          cleanupResume(Effect.void);
        });
      });
    }),
    (server) =>
      Effect.callback<void>((resume) => {
        server.close(() => {
          server.removeAllListeners();
          resume(Effect.void);
        });
      }),
    { interruptible: true },
  );
  const address = yield* Schema.decodeUnknownEffect(TcpAddress)(server.address());
  return address.port;
});

export const sendMalformedWebSocketUpgrade = Effect.fn(
  "ViewServerServer.test.websocket.malformedUpgrade",
)(function* (url: string) {
  const target = new URL(url.replace("ws://", "http://"));
  yield* Effect.callback<void, ServerTestMalformedUpgradeError>((resume, signal) => {
    const socket = Net.createConnection({
      host: target.hostname,
      port: Number(target.port),
    });
    const cleanup = () => {
      socket.removeAllListeners();
      socket.destroy();
    };
    const succeed = () => {
      cleanup();
      resume(Effect.void);
    };
    const fail = (cause: unknown) => {
      cleanup();
      resume(Effect.fail(new ServerTestMalformedUpgradeError({ cause })));
    };

    signal.addEventListener("abort", cleanup, { once: true });
    socket.once("connect", () => {
      socket.write(
        [
          `GET ${target.pathname} HTTP/1.1`,
          `Host: ${target.host}`,
          "Connection: Upgrade",
          "Upgrade: websocket",
          "",
          "",
        ].join("\r\n"),
        () => {
          socket.destroy();
        },
      );
    });
    socket.once("close", succeed);
    socket.once("error", fail);
    return Effect.sync(cleanup);
  });
});

export const openRawWebSocket = Effect.fn("ViewServerServer.test.websocket.raw.open")(function* (
  url: string,
) {
  return yield* Effect.callback<globalThis.WebSocket, ServerTestWebSocketOpenError>(
    (resume, signal) => {
      const socket = new WebSocket(url);
      const cleanup = () => {
        socket.removeEventListener("open", opened);
        socket.removeEventListener("error", failed);
        signal.removeEventListener("abort", aborted);
      };
      function opened() {
        cleanup();
        resume(Effect.succeed(socket));
      }
      function failed(cause: Event) {
        cleanup();
        socket.close();
        resume(Effect.fail(new ServerTestWebSocketOpenError({ cause })));
      }
      function aborted() {
        cleanup();
        socket.close();
      }

      signal.addEventListener("abort", aborted, { once: true });
      socket.addEventListener("open", opened, { once: true });
      socket.addEventListener("error", failed, { once: true });
      return Effect.sync(aborted);
    },
  );
});

export type ServerTransportLifecycleCounts = {
  readonly openedClients: number;
  readonly closedClients: number;
  readonly openedStreams: number;
  readonly closedStreams: number;
};

export const awaitServerTransportLifecycleCount = Effect.fn(
  "ViewServerServer.test.transport.awaitCount",
)(function* (
  counts: SubscriptionRef.SubscriptionRef<ServerTransportLifecycleCounts>,
  field: keyof ServerTransportLifecycleCounts,
  expected: number,
) {
  yield* SubscriptionRef.changes(counts).pipe(
    Stream.filter((current) => current[field] === expected),
    Stream.take(1),
    Stream.runDrain,
    Effect.timeout("1 second"),
  );
});

export const makeServerTransportLifecycleProbe = Effect.fn(
  "ViewServerServer.test.transport.probe.make",
)(function* () {
  const counts = yield* SubscriptionRef.make<ServerTransportLifecycleCounts>({
    openedClients: 0,
    closedClients: 0,
    openedStreams: 0,
    closedStreams: 0,
  });

  return {
    awaitCount: (field: keyof ServerTransportLifecycleCounts, expected: number) =>
      awaitServerTransportLifecycleCount(counts, field, expected),
    readCounts: SubscriptionRef.get(counts),
    transport: {
      clientOpened: SubscriptionRef.update(counts, (current) => ({
        ...current,
        openedClients: current.openedClients + 1,
      })),
      clientClosed: SubscriptionRef.update(counts, (current) => ({
        ...current,
        closedClients: current.closedClients + 1,
      })),
      streamOpened: SubscriptionRef.update(counts, (current) => ({
        ...current,
        openedStreams: current.openedStreams + 1,
      })),
      streamClosed: SubscriptionRef.update(counts, (current) => ({
        ...current,
        closedStreams: current.closedStreams + 1,
      })),
    },
  };
});
