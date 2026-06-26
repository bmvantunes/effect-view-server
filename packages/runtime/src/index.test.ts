import { describe, expect, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import type { Message } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import type { ColumnLiveViewEngineHealth } from "@view-server/column-live-view-engine";
import { makeViewServerClient } from "@view-server/client/remote";
import {
  defineViewServerConfig,
  grpc,
  kafka,
  type TransportHealth,
  type ViewServerHealth,
  type ViewServerRuntimeError,
  type ViewServerRuntimeClient,
} from "@view-server/config";
import { makeViewServerRuntimeCore } from "@view-server/runtime-core";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import {
  Cause,
  Clock,
  Config,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Schedule,
  Schema,
  Stream,
} from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import {
  makeDefaultRuntimeDependencies,
  makeViewServerRuntimeWithDependencies,
  runViewServerRuntimeWithDependencies,
} from "./internal";
import { makeViewServerGrpcHealthLedger } from "./grpc-health";
import { makeViewServerGrpcIngress, ViewServerGrpcIngressError } from "./grpc-ingress";
import { makeViewServerRuntime, runViewServerRuntime } from "./index";
import { ViewServerKafkaIngressError } from "./kafka-ingress";
import {
  resolveViewServerRuntimeOptions,
  validateSourceOwnership,
  type ResolvedViewServerGrpcRuntimeOptions,
  type ResolvedViewServerKafkaRuntimeOptions,
} from "./runtime-options";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const HealthJson = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  engine: Schema.Struct({
    topics: Schema.Struct({
      orders: Schema.Struct({
        rowCount: Schema.Number,
      }),
    }),
  }),
});

class RuntimeHealthJsonParseError extends Schema.TaggedErrorClass<RuntimeHealthJsonParseError>()(
  "RuntimeHealthJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

type OrderRow = typeof Order.Type;

type GrpcOrderValueMessage = Message<"viewserver.runtime.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

type GrpcOrderKeyMessage = Message<"viewserver.runtime.OrderKey"> & {
  readonly orderId: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const runtimeGrpcProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/runtime.proto",
        package: "viewserver.runtime",
        syntax: "proto3",
        messageType: [
          {
            name: "OrderValue",
            field: [
              { name: "customer_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "updated_at", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "OrderKey",
            field: [{ name: "order_id", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
        ],
        service: [
          {
            name: "OrdersService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.runtime.OrderKey",
                outputType: ".viewserver.runtime.OrderValue",
                serverStreaming: true,
              },
            ],
          },
        ],
      }),
    ),
  ),
);

const grpcOrderValueSchema = messageDesc<GrpcOrderValueMessage>(runtimeGrpcProtoFile, 0);
const grpcOrderKeySchema = messageDesc<GrpcOrderKeyMessage>(runtimeGrpcProtoFile, 1);
const grpcOrdersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof grpcOrderKeySchema;
    readonly output: typeof grpcOrderValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(runtimeGrpcProtoFile, 0);

const GrpcOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const grpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      source: grpc.materialized(),
    },
  },
});

type GrpcTopics = typeof grpcViewServer.topics;

const grpcAndKafkaViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      source: grpc.materialized(),
    },
    audit: {
      schema: Order,
      key: "id",
    },
  },
});

const leasedGrpcViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: GrpcOrder,
      key: "id",
      source: grpc.leased({
        routeBy: ["region"],
      }),
    },
  },
});

const grpcClients = {
  orders: grpc.connectClient({
    service: grpcOrdersService,
    baseUrl: Config.succeed("https://orders.example.test"),
  }),
};

const grpcClientsWithOrphan = {
  ...grpcClients,
  orphan: grpc.connectClient({
    service: grpcOrdersService,
    baseUrl: Config.succeed("https://orphan.example.test"),
  }),
};

const grpcFeed = grpcViewServer.grpcFeed<typeof grpcClients>();
const grpcFeedWithOrphan = grpcViewServer.grpcFeed<typeof grpcClientsWithOrphan>();
const mixedGrpcFeed = grpcAndKafkaViewServer.grpcFeed<typeof grpcClients>();
const leasedGrpcFeed = leasedGrpcViewServer.grpcFeed<typeof grpcClients>();

const grpcOrderValue = (
  customerId: string,
  price: number,
  status: GrpcOrderValueMessage["status"] = "open",
): GrpcOrderValueMessage => ({
  $typeName: "viewserver.runtime.OrderValue",
  customerId,
  status,
  price,
  updatedAt: price,
});

const grpcMaterializedFeed = (stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>) =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => stream,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcMaterializedFeedWithRelease = (
  stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>,
  release: Effect.Effect<void>,
) =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => stream,
    release: () => release,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcMaterializedFeedWithRequestFailure = () =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => {
      throw new Error("request exploded");
    },
    acquire: () => Stream.never,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcMaterializedFeedWithAcquireFailure = () =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => {
      throw new Error("acquire exploded");
    },
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const grpcMaterializedFeedWithMappingFailure = (
  stream: Stream.Stream<GrpcOrderValueMessage, unknown, never>,
) =>
  grpcFeed.materializedFeed({
    topic: "orders",
    client: "orders",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => stream,
    map: () => {
      throw new Error("mapping exploded");
    },
  });

const grpcMaterializedFeedWithOrphanClient = () =>
  grpcFeedWithOrphan.materializedFeed({
    topic: "orders",
    client: "orphan",
    method: "streamOrders",
    request: () => ({ orderId: "all" }),
    acquire: () => Stream.never,
    map: ({ value }) => ({
      id: value.customerId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region: "usa",
      updatedAt: value.updatedAt,
    }),
  });

const longRunningGrpcStream = (
  values: ReadonlyArray<GrpcOrderValueMessage>,
): Stream.Stream<GrpcOrderValueMessage, never, never> =>
  Stream.make(...values).pipe(Stream.concat(Stream.never));

const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

const nullRecord = <Value>(
  entries: ReadonlyArray<readonly [string, Value]>,
): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  for (const [key, value] of entries) {
    record[key] = value;
  }
  return record;
};

const fetchHealth = Effect.fn("ViewServerRuntime.test.health.fetch")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new RuntimeHealthJsonParseError({ cause }),
  });
  const health = yield* Schema.decodeUnknownEffect(HealthJson)(value);
  return { response, health };
});

const waitForTransportHealth = Effect.fn("ViewServerRuntime.test.transportHealth.wait")(function* (
  health: () => Effect.Effect<{ readonly transport: TransportHealth }, unknown>,
  expected: {
    readonly activeClients: number;
    readonly activeStreams: number;
  },
) {
  return yield* health().pipe(
    Effect.map((value) => value.transport),
    Effect.repeat({
      schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
      until: (transport) =>
        transport.activeClients === expected.activeClients &&
        transport.activeStreams === expected.activeStreams,
    }),
  );
});

const waitForGrpcSnapshotRows = Effect.fn("ViewServerRuntime.test.grpc.snapshotRows.wait")(
  function* (client: ViewServerRuntimeClient<GrpcTopics>, expectedTotalRows: number) {
    return yield* client
      .snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      })
      .pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (snapshot) => snapshot.totalRows === expectedTotalRows,
        }),
      );
  },
);

const readGrpcHealthOverlay = Effect.fn("ViewServerRuntime.test.grpc.healthOverlay.read")(
  function* (
    client: ViewServerRuntimeClient<GrpcTopics>,
    health: ReturnType<typeof makeViewServerGrpcHealthLedger<GrpcTopics>>,
    nowMillis: number,
  ) {
    return health.healthOverlay(yield* client.health(), nowMillis);
  },
);

const readGrpcHealthOverlayNow = Effect.fn("ViewServerRuntime.test.grpc.healthOverlay.readNow")(
  function* (
    client: ViewServerRuntimeClient<GrpcTopics>,
    health: ReturnType<typeof makeViewServerGrpcHealthLedger<GrpcTopics>>,
  ) {
    const nowMillis = yield* Clock.currentTimeMillis;
    return health.healthOverlay(yield* client.health(), nowMillis);
  },
);

const makeGrpcHealth = (
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<GrpcTopics, typeof grpcClients>,
) =>
  makeViewServerGrpcHealthLedger<GrpcTopics>({
    clients: grpcOptions.clientBaseUrls,
    feeds: {
      ordersFeed: {
        client: "orders",
        lifecycle: "materialized",
        topic: "orders",
      },
    },
  });

const grpcHealthFeed = (health: ViewServerHealth<GrpcTopics>) =>
  health.grpc?.feeds["orders"]?.materialized["ordersFeed"];

const grpcHealthClient = (health: ViewServerHealth<GrpcTopics>) => health.grpc?.clients["orders"];

const resolveGrpcRuntimeOptions = Effect.fn("ViewServerRuntime.test.grpc.options.resolve")(
  function* (feed: ReturnType<typeof grpcMaterializedFeed>) {
    const options = yield* resolveViewServerRuntimeOptions<
      GrpcTopics,
      Record<string, string>,
      typeof grpcClients
    >({
      grpc: {
        clients: grpcClients,
        feeds: {
          ordersFeed: feed,
        },
      },
    });
    return yield* Effect.fromNullishOr(options.grpcOptions);
  },
);

describe("@view-server/runtime", () => {
  it.live("starts a websocket runtime with health endpoint and runtime-core mutation client", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        rpcPath: "/runtime-rpc",
        healthPath: "/runtime-health",
      });
      const remoteClient = yield* makeViewServerClient(viewServer, { url: runtime.url });
      const subscription = yield* remoteClient.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const connectedTransport = yield* waitForTransportHealth(runtime.client.health, {
        activeClients: 1,
        activeStreams: 1,
      });
      expect(runtime.liveClient.health.value.transport.activeStreams).toBe(1);
      expect(connectedTransport).toStrictEqual({
        activeClients: 1,
        activeStreams: 1,
        activeSubscriptions: 1,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });

      yield* runtime.client.publish("orders", order("a", 10));

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
        totalRows: 1,
      });

      const health = yield* fetchHealth(runtime.healthUrl);
      expect(runtime.url.endsWith("/runtime-rpc")).toBe(true);
      expect(runtime.healthUrl.endsWith("/runtime-health")).toBe(true);
      expect(health.response.status).toBe(200);
      expect(health.health.engine.topics.orders.rowCount).toBe(1);

      yield* subscription.close();
      yield* remoteClient.close;
      const disconnectedTransport = yield* waitForTransportHealth(runtime.client.health, {
        activeClients: 0,
        activeStreams: 0,
      });
      expect(disconnectedTransport).toStrictEqual({
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });
      yield* runtime.close;
    }),
  );

  it.live("supports default paths and queue capacity options", () =>
    Effect.gen(function* () {
      const defaultRuntime = yield* makeViewServerRuntime(viewServer);
      expect(defaultRuntime.url.endsWith("/rpc")).toBe(true);
      expect(defaultRuntime.healthUrl.endsWith("/health")).toBe(true);
      expect("subscribeRuntime" in defaultRuntime.liveClient).toBe(false);
      yield* defaultRuntime.close;

      const configuredRuntime = yield* makeViewServerRuntime(viewServer, {
        websocketPort: 0,
        subscriptionQueueCapacity: 1,
      });
      expect(configuredRuntime.url.endsWith("/rpc")).toBe(true);
      expect(configuredRuntime.healthUrl.endsWith("/health")).toBe(true);
      yield* configuredRuntime.close;
    }),
  );

  it.effect("tracks runtime transport stream health", () =>
    Effect.gen(function* () {
      const transport = makeViewServerRuntimeTransportHealth<typeof viewServer.topics>();
      const engineHealth = {
        status: "ready",
        version: 1,
        topics: {
          orders: {
            status: "ready",
            rowCount: 10,
            liveRowCount: 10,
            deletedRowCount: 0,
            version: 3,
            lastMutationAt: 1,
            mutationsPerSecond: 2,
            rowsPerSecond: 2,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 1,
            activeViews: 1,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 4,
            queuedEvents: 5,
            maxQueueDepth: 6,
            backpressureEvents: 7,
            memoryBytes: 8,
            tombstoneCount: 0,
            compactionPending: false,
          },
        },
        activeSubscriptions: 4,
        queuedEvents: 5,
        maxQueueDepth: 6,
        backpressureEvents: 7,
      } satisfies ColumnLiveViewEngineHealth<typeof viewServer.topics>;

      expect(transport.transportHealth(engineHealth).activeStreams).toBe(0);
      expect(transport.transportHealth(engineHealth).activeClients).toBe(0);
      yield* transport.clientOpened;
      yield* transport.streamOpened;
      yield* transport.streamOpened;
      expect(transport.transportHealth(engineHealth)).toStrictEqual({
        activeClients: 1,
        activeStreams: 2,
        activeSubscriptions: 4,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 5,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 7,
        reconnects: 0,
        lastError: null,
      });
      yield* transport.streamClosed;
      yield* transport.streamClosed;
      yield* transport.streamClosed;
      expect(transport.transportHealth(engineHealth).activeStreams).toBe(0);
      expect(transport.transportHealth(engineHealth).activeClients).toBe(1);
      yield* transport.clientClosed;
      yield* transport.clientClosed;
      expect(transport.transportHealth(engineHealth).activeClients).toBe(0);
    }),
  );

  it.live("forwards runtime options to the runtime core and websocket server", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      let runtimeCoreOptions: Parameters<RuntimeDependencies["makeRuntimeCore"]>[1] | undefined;
      let serverInput: Parameters<RuntimeDependencies["makeServer"]>[1] | undefined;
      let serverOptions: Parameters<RuntimeDependencies["makeServer"]>[2] | undefined;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) => {
          runtimeCoreOptions = options;
          return makeViewServerRuntimeCore(config, options);
        },
        makeServer: (_config, input, options) => {
          serverInput = input;
          serverOptions = options;
          return Effect.succeed({
            url: "ws://127.0.0.1:0/custom-rpc",
            healthUrl: "http://127.0.0.1:0/custom-health",
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
        host: "0.0.0.0",
        websocketPort: 1234,
        rpcPath: "/custom-rpc",
        healthPath: "/custom-health",
        subscriptionQueueCapacity: 7,
      });

      expect({
        runtimeCoreOptions: {
          subscriptionQueueCapacity: runtimeCoreOptions?.subscriptionQueueCapacity,
          groupedIncrementalAdmissionLimits: runtimeCoreOptions?.groupedIncrementalAdmissionLimits,
          transportHealthType: typeof runtimeCoreOptions?.transportHealth,
        },
        serverTransportHooks: {
          clientOpenedType: typeof serverInput?.transport?.clientOpened,
          clientClosedType: typeof serverInput?.transport?.clientClosed,
          streamOpenedType: typeof serverInput?.transport?.streamOpened,
          streamClosedType: typeof serverInput?.transport?.streamClosed,
        },
        serverOptions,
      }).toStrictEqual({
        runtimeCoreOptions: {
          subscriptionQueueCapacity: 7,
          groupedIncrementalAdmissionLimits: {
            maxGroups: 1,
          },
          transportHealthType: "function",
        },
        serverTransportHooks: {
          clientOpenedType: "object",
          clientClosedType: "object",
          streamOpenedType: "object",
          streamClosedType: "object",
        },
        serverOptions: {
          host: "0.0.0.0",
          port: 1234,
          path: "/custom-rpc",
          healthPath: "/custom-health",
        },
      });
      yield* runtime.close;
    }),
  );

  it.live("resolves Kafka runtime options and starts configured ingress", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const regions = {
        local: Config.succeed("localhost:9092"),
      };
      let kafkaOptionsSummary:
        | {
            readonly consume: ResolvedViewServerKafkaRuntimeOptions<
              typeof viewServer.topics
            >["consume"];
            readonly consumerGroupId: string;
            readonly regions: Readonly<Record<string, string>>;
            readonly startFrom: ResolvedViewServerKafkaRuntimeOptions<
              typeof viewServer.topics
            >["startFrom"];
            readonly topics: Readonly<
              Record<
                string,
                { readonly regions: ReadonlyArray<string>; readonly viewServerTopic: string }
              >
            >;
          }
        | undefined;
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.void,
          }),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, options) => {
          kafkaOptionsSummary = {
            consume: options.consume,
            consumerGroupId: options.consumerGroupId,
            regions: options.regions,
            startFrom: options.startFrom,
            topics: Object.fromEntries(
              Object.entries(options.topics).map(([sourceTopic, topic]) => [
                sourceTopic,
                {
                  regions: topic.regions,
                  viewServerTopic: topic.viewServerTopic,
                },
              ]),
            ),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-test-runtime",
          regions,
          topics: {
            "orders-source": localKafkaTopic({
              regions: ["local"],
              value: kafka.json(Order),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              mapping: ({ key, value }) => ({
                id: key,
                price: value.price,
              }),
            }),
          },
        },
      });

      expect({
        consume: kafkaOptionsSummary?.consume,
        consumerGroupId: kafkaOptionsSummary?.consumerGroupId,
        regions: kafkaOptionsSummary?.regions,
        startFrom: kafkaOptionsSummary?.startFrom,
        topics: kafkaOptionsSummary?.topics,
      }).toStrictEqual({
        consume: {
          consumerGroupId: "view-server-test-runtime",
          fallbackMode: "earliest",
          mode: "committed",
        },
        consumerGroupId: "view-server-test-runtime",
        regions: nullRecord([["local", "localhost:9092"]]),
        startFrom: {
          committedConsumerGroup: "view-server-test-runtime",
        },
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* runtime.close;
    }),
  );

  it.live("resolves gRPC runtime options and starts configured materialized ingress", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<GrpcTopics>;
      const feed = grpcMaterializedFeed(Stream.never);
      let grpcOptionsSummary:
        | {
            readonly clientBaseUrls: Readonly<Record<string, string>>;
            readonly clientNames: ReadonlyArray<string>;
            readonly feeds: Readonly<
              Record<
                string,
                {
                  readonly client: string;
                  readonly lifecycle: string;
                  readonly method: string;
                  readonly topic: string;
                }
              >
            >;
          }
        | undefined;
      let grpcHealthLedgerCreated = false;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<GrpcTopics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.void,
          }),
        makeGrpcHealthLedger: (config, options) => {
          grpcHealthLedgerCreated =
            config === grpcViewServer &&
            options.clientBaseUrls["orders"] === "https://orders.example.test";
          return makeDefaultRuntimeDependencies<GrpcTopics>().makeGrpcHealthLedger(config, options);
        },
        makeGrpcIngress: (_config, _client, _requestHealthRefresh, options) => {
          grpcOptionsSummary = {
            clientBaseUrls: options.clientBaseUrls,
            clientNames: Object.keys(options.clients),
            feeds: Object.fromEntries(
              Object.entries(options.feeds).map(([feedName, resolvedFeed]) => [
                feedName,
                {
                  client: resolvedFeed.client,
                  lifecycle: resolvedFeed.lifecycle,
                  method: resolvedFeed.method,
                  topic: resolvedFeed.topic,
                },
              ]),
            ),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const grpcRuntimeOptions = {
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersFeed: feed,
          },
        },
      };
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        grpcViewServer,
        grpcRuntimeOptions,
      );

      expect(grpcHealthLedgerCreated).toBe(true);
      expect({
        clientBaseUrls: grpcOptionsSummary?.clientBaseUrls,
        clientNames: grpcOptionsSummary?.clientNames,
        feeds: grpcOptionsSummary?.feeds,
      }).toStrictEqual({
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        clientNames: ["orders"],
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            method: "streamOrders",
            topic: "orders",
          },
        },
      });

      yield* runtime.close;
    }),
  );

  it.live("rejects multiple gRPC feeds targeting the same View Server topic", () =>
    Effect.gen(function* () {
      const firstFeed = grpcMaterializedFeed(Stream.never);
      const secondFeed = grpcFeedWithOrphan.materializedFeed({
        topic: "orders",
        client: "orphan",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.never,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });

      const error = yield* makeViewServerRuntime(grpcViewServer, {
        grpc: {
          clients: grpcClientsWithOrphan,
          feeds: {
            ordersFeed: firstFeed,
            secondOrdersFeed: secondFeed,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "gRPC feed secondOrdersFeed conflicts with ordersFeed; View Server topic orders already has a gRPC feed owner.",
      );
    }),
  );

  it.live("rejects leased gRPC feeds until the runtime lease manager exists", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });

      const error = yield* makeViewServerRuntime(leasedGrpcViewServer, {
        grpc: {
          clients: grpcClients,
          feeds: {
            ordersLease: feed,
          },
        },
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "gRPC leased feed ordersLease is not supported by runtime startup yet.",
      );
    }),
  );

  it.effect("rejects resolved Kafka and gRPC ownership of the same View Server topic", () =>
    Effect.gen(function* () {
      const error = yield* validateSourceOwnership(
        {
          topics: {
            "orders-source": {
              viewServerTopic: "orders",
            },
          },
        },
        {
          feeds: {
            ordersFeed: {
              topic: "orders",
            },
          },
        },
      ).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ViewServerGrpcIngressError);
      expect(error.message).toBe(
        "View Server topic orders cannot be owned by both Kafka source orders-source and gRPC feed ordersFeed.",
      );
    }),
  );

  it.live("closes started resources when gRPC ingress startup fails", () =>
    Effect.gen(function* () {
      type MixedTopics = typeof grpcAndKafkaViewServer.topics;
      type RuntimeDependencies = ViewServerRuntimeDependencies<MixedTopics>;
      const feed = mixedGrpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all" }),
        acquire: () => Stream.never,
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = grpcAndKafkaViewServer.kafkaTopic<typeof regions>();
      let serverClosed = 0;
      let kafkaClosed = 0;
      let runtimeCoreClosed = 0;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<MixedTopics>(),
        makeRuntimeCore: (config, options) =>
          makeDefaultRuntimeDependencies<MixedTopics>()
            .makeRuntimeCore(config, options)
            .pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                close: runtimeCore.close.pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      runtimeCoreClosed += 1;
                    }),
                  ),
                ),
              })),
            ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.sync(() => {
              serverClosed += 1;
            }),
          }),
        makeKafkaIngress: () =>
          Effect.succeed({
            close: Effect.sync(() => {
              kafkaClosed += 1;
            }),
          }),
        makeGrpcIngress: () =>
          Effect.fail(
            new ViewServerGrpcIngressError({
              message: "gRPC failed during startup",
              cause: "startup",
            }),
          ),
      };

      const exit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, grpcAndKafkaViewServer, {
          kafka: {
            consumerGroupId: "view-server-grpc-startup-failure",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "audit",
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersFeed: feed,
            },
          },
        }),
      );

      expect({
        failed: Exit.isFailure(exit),
        kafkaClosed,
        runtimeCoreClosed,
        serverClosed,
      }).toStrictEqual({
        failed: true,
        kafkaClosed: 1,
        runtimeCoreClosed: 1,
        serverClosed: 1,
      });
    }),
  );

  it.live("closes server and runtime core when gRPC ingress startup fails without Kafka", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<GrpcTopics>;
      const feed = grpcMaterializedFeed(Stream.never);
      let serverClosed = 0;
      let runtimeCoreClosed = 0;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<GrpcTopics>(),
        makeRuntimeCore: (config, options) =>
          makeDefaultRuntimeDependencies<GrpcTopics>()
            .makeRuntimeCore(config, options)
            .pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                close: runtimeCore.close.pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      runtimeCoreClosed += 1;
                    }),
                  ),
                ),
              })),
            ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.sync(() => {
              serverClosed += 1;
            }),
          }),
        makeGrpcIngress: () =>
          Effect.fail(
            new ViewServerGrpcIngressError({
              message: "gRPC failed before Kafka existed",
              cause: "startup",
            }),
          ),
      };

      const exit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, grpcViewServer, {
          grpc: {
            clients: grpcClients,
            feeds: {
              ordersFeed: feed,
            },
          },
        }),
      );

      expect({
        failed: Exit.isFailure(exit),
        runtimeCoreClosed,
        serverClosed,
      }).toStrictEqual({
        failed: true,
        runtimeCoreClosed: 1,
        serverClosed: 1,
      });
    }),
  );

  it.live("tracks gRPC materialized feed health and same-window rate increments", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });

      const startingHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      yield* health.clientConnected("missing", 1_000);
      yield* health.clientDegraded("missing", "ignored");
      yield* health.feedReady("missing");
      yield* health.feedStopping("missing");
      yield* health.feedDegraded("missing", "ignored");
      yield* health.rowsPublished("missing", {
        messages: 1,
        rows: 1,
        nowMillis: 2_000,
      });
      yield* health.mappingFailed("missing", {
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* health.publishFailed("missing", {
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* health.clientConnected("orders", 1_000);
      yield* health.feedReady("ordersFeed");
      yield* health.rowsPublished("ordersFeed", {
        messages: 1,
        rows: 2,
        nowMillis: 2_000,
      });
      yield* health.rowsPublished("ordersFeed", {
        messages: 3,
        rows: 4,
        nowMillis: 2_000,
      });
      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        startingStatus: startingHealth.status,
        startingActiveFeeds: startingHealth.grpc?.clients["orders"]?.activeFeeds,
        readyStatus: readyHealth.status,
        readyActiveFeeds: readyHealth.grpc?.clients["orders"]?.activeFeeds,
        materialized: readyHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"],
      }).toStrictEqual({
        startingStatus: "starting",
        startingActiveFeeds: 0,
        readyStatus: "ready",
        readyActiveFeeds: 1,
        materialized: {
          status: "ready",
          lifecycle: "materialized",
          feedName: "ordersFeed",
          feedKey: "orders/ordersFeed/materialized",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 4,
          rowsPerSecond: 6,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: 2_000,
          lastError: null,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.live("tracks leased gRPC feed health in the ledger without starting runtime leases", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(leasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: {
          orders: "https://orders.example.test",
        },
        feeds: {
          ordersLease: {
            client: "orders",
            lifecycle: "leased",
            topic: "orders",
          },
        },
      });

      const startingHealth = health.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      yield* health.clientConnected("orders", 1_000);
      yield* health.feedReady("ordersLease");
      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        startingActiveFeeds: startingHealth.grpc?.clients["orders"]?.activeFeeds,
        startingStatus: startingHealth.grpc?.feeds["orders"]?.leased["ordersLease"]?.status,
        readyActiveFeeds: readyHealth.grpc?.clients["orders"]?.activeFeeds,
        readyFeed: readyHealth.grpc?.feeds["orders"]?.leased["ordersLease"],
      }).toStrictEqual({
        startingActiveFeeds: 0,
        startingStatus: "starting",
        readyActiveFeeds: 1,
        readyFeed: {
          status: "ready",
          lifecycle: "leased",
          feedName: "ordersLease",
          feedKey: "orders/ordersLease/leased",
          topic: "orders",
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when the stream completes", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.make(grpcOrderValue("order-1", 10)));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed completed unexpectedly.",
      );
      expect(grpcHealthClient(degradedHealth)?.activeFeeds).toBe(0);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed stopping when the stream is interrupted", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.failCause(Cause.interrupt()));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const stoppingHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "stopping",
        }),
      );

      expect(grpcHealthFeed(stoppingHealth)?.lastError).toBe(null);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("publishes materialized gRPC stream rows into runtime core and health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(
        longRunningGrpcStream([grpcOrderValue("order-1", 10), grpcOrderValue("order-2", 5)]),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      let healthRefreshCount = 0;
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.sync(() => {
          healthRefreshCount += 1;
        }),
        grpcOptions,
        health,
      );

      const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 2);
      const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      const clientHealth = currentHealth.grpc?.clients["orders"];
      const feedHealth = currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"];

      expect(snapshot).toStrictEqual({
        rows: [
          { id: "order-2", price: 5 },
          { id: "order-1", price: 10 },
        ],
        totalRows: 2,
        version: 1,
        status: "ready",
        statusCode: "Ready",
      });
      expect(clientHealth).toStrictEqual({
        status: "connected",
        baseUrl: "https://orders.example.test",
        activeFeeds: 1,
        lastConnectedAt: clientHealth?.lastConnectedAt,
        lastError: null,
      });
      expect(feedHealth).toStrictEqual({
        status: "ready",
        lifecycle: "materialized",
        feedName: "ordersFeed",
        feedKey: "orders/ordersFeed/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 2,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: feedHealth?.lastMessageAt,
        lastError: null,
      });
      expect(typeof clientHealth?.lastConnectedAt).toBe("number");
      expect(typeof feedHealth?.lastMessageAt).toBe("number");
      expect(healthRefreshCount).toBe(2);

      yield* ingress.close;
      const stoppedHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect(stoppedHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status).toBe(
        "stopping",
      );
      yield* runtimeCore.close;
    }),
  );

  it.live(
    "reports materialized gRPC row count from engine health instead of cumulative publishes",
    () =>
      Effect.gen(function* () {
        const feed = grpcMaterializedFeed(
          longRunningGrpcStream([
            grpcOrderValue("order-1", 10),
            grpcOrderValue("order-1", 20),
            grpcOrderValue("order-1", 30),
          ]),
        );
        const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
        const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
        const health = makeGrpcHealth(grpcOptions);
        const ingress = yield* makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        );

        const snapshot = yield* waitForGrpcSnapshotRows(runtimeCore.client, 1);
        const currentHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);

        expect(snapshot).toStrictEqual({
          rows: [{ id: "order-1", price: 30 }],
          totalRows: 1,
          version: 1,
          status: "ready",
          statusCode: "Ready",
        });
        expect(currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.rowCount).toBe(1);

        yield* ingress.close;
        yield* runtimeCore.close;
      }),
  );

  it.live("marks materialized gRPC feed degraded when the stream defects", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.fromEffect(Effect.die("defect down")));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("gRPC feed ordersFeed failed:");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toContain("defect down");
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed health degraded when the stream fails", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.fail("upstream down"));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      let healthRefreshCount = 0;
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.sync(() => {
          healthRefreshCount += 1;
        }),
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlay(runtimeCore.client, health, 2_000).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) =>
            currentHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(degradedHealth.grpc?.clients["orders"]?.status).toBe("degraded");
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.status).toBe(
        "degraded",
      );
      expect(degradedHealth.grpc?.feeds["orders"]?.materialized["ordersFeed"]?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed stream failed for ordersFeed: upstream down",
      );
      expect(healthRefreshCount).toBe(2);

      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("releases materialized gRPC feed resources when ingress closes", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const feed = grpcMaterializedFeedWithRelease(
        Stream.never,
        Deferred.succeed(released, undefined),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* ingress.close;
      yield* Deferred.await(released);
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000)),
      ).toStrictEqual({
        status: "stopping",
        lifecycle: "materialized",
        feedName: "ordersFeed",
        feedKey: "orders/ordersFeed/materialized",
        topic: "orders",
        subscriberCount: 0,
        rowCount: 0,
        messagesPerSecond: 0,
        rowsPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        publishFailuresPerSecond: 0,
        reconnects: 0,
        lastMessageAt: null,
        lastError: null,
      });
      yield* runtimeCore.close;
    }),
  );

  it.live("ignores materialized gRPC release construction failures during ingress close", () =>
    Effect.gen(function* () {
      const feed = grpcFeed.materializedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        request: () => ({ orderId: "all-orders" }),
        acquire: () => Stream.never,
        release: () => {
          throw new Error("release exploded");
        },
        map: ({ value }) => ({
          id: value.customerId,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: "usa",
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      yield* ingress.close;
      expect(
        grpcHealthFeed(health.healthOverlay(yield* runtimeCore.client.health(), 2_000))?.status,
      ).toBe("stopping");
      yield* runtimeCore.close;
    }),
  );

  it.live("refreshes materialized gRPC health after an idle feed becomes ready", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      let healthRefreshCount = 0;
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.sync(() => {
          healthRefreshCount += 1;
        }),
        grpcOptions,
        health,
      );

      const readyHealth = health.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      expect(grpcHealthFeed(readyHealth)?.status).toBe("ready");
      expect(grpcHealthClient(readyHealth)?.activeFeeds).toBe(1);
      expect(healthRefreshCount).toBe(1);
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when request creation fails", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithRequestFailure();
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error._tag).toBe("ViewServerGrpcIngressError");
      expect(error.message).toBe("gRPC feed request creation failed for ordersFeed");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when client creation throws", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
          () => {
            throw new Error("client factory exploded");
          },
        ),
      );

      expect(error._tag).toBe("ViewServerGrpcIngressError");
      expect(error.message).toBe("gRPC client creation failed for ordersFeed");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails lower-level gRPC ingress startup for leased feeds", () =>
    Effect.gen(function* () {
      const feed = leasedGrpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["region"],
        request: ({ region }) => ({ orderId: region }),
        acquire: () => Stream.never,
        map: ({ value, route }) => ({
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        }),
      });
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<
        typeof leasedGrpcViewServer.topics,
        typeof grpcClients
      > = {
        clients: grpcClients,
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        feeds: {
          ordersLease: feed,
        },
      };
      const runtimeCore = yield* makeViewServerRuntimeCore(leasedGrpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<typeof leasedGrpcViewServer.topics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersLease: {
            client: "orders",
            lifecycle: "leased",
            topic: "orders",
          },
        },
      });

      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          leasedGrpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error.message).toBe(
        "gRPC leased feed ordersLease is not supported by materialized ingress startup",
      );
      expect(error.feedName).toBe("ordersLease");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("closes already-started gRPC feed resources when another feed fails startup", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const runningFeed = grpcMaterializedFeedWithRelease(
        Stream.never,
        Deferred.succeed(released, undefined),
      );
      const failingFeed = grpcMaterializedFeedWithRequestFailure();
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<GrpcTopics, typeof grpcClients> = {
        clients: grpcClients,
        clientBaseUrls: nullRecord([["orders", "https://orders.example.test"]]),
        feeds: {
          runningFeed,
          failingFeed,
        },
      };
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          runningFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
          failingFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });

      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      yield* Deferred.await(released);
      expect(error.message).toBe("gRPC feed request creation failed for failingFeed");
      expect(error.feedName).toBe("failingFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when feed client is missing", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithOrphanClient();
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<
        GrpcTopics,
        typeof grpcClientsWithOrphan
      > = {
        // @ts-expect-error defensive runtime-boundary test intentionally omits the orphan client.
        clients: {},
        clientBaseUrls: {},
        feeds: {
          ordersFeed: feed,
        },
      };
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error.message).toBe("gRPC feed ordersFeed references missing client: orphan");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("fails materialized gRPC ingress startup when feed client URL is unresolved", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(Stream.never);
      const grpcOptions: ResolvedViewServerGrpcRuntimeOptions<GrpcTopics, typeof grpcClients> = {
        clients: grpcClients,
        clientBaseUrls: {},
        feeds: {
          ordersFeed: feed,
        },
      };
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeViewServerGrpcHealthLedger<GrpcTopics>({
        clients: grpcOptions.clientBaseUrls,
        feeds: {
          ordersFeed: {
            client: "orders",
            lifecycle: "materialized",
            topic: "orders",
          },
        },
      });
      const error = yield* Effect.flip(
        makeViewServerGrpcIngress(
          grpcViewServer,
          runtimeCore.client,
          Effect.void,
          grpcOptions,
          health,
        ),
      );

      expect(error.message).toBe("gRPC feed ordersFeed references unresolved client URL: orders");
      expect(error.feedName).toBe("ordersFeed");
      expect(error.topic).toBe("orders");
      yield* runtimeCore.close;
    }),
  );

  it.live("marks materialized gRPC feed degraded when acquire throws", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithAcquireFailure();
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthClient(degradedHealth)?.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed acquire failed for ordersFeed: acquire exploded",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("records materialized gRPC mapping failures in health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeedWithMappingFailure(
        longRunningGrpcStream([grpcOrderValue("order-1", 10)]),
      );
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        runtimeCore.client,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed mapping failed for ordersFeed: mapping exploded",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.live("records materialized gRPC publish failures in health", () =>
    Effect.gen(function* () {
      const feed = grpcMaterializedFeed(longRunningGrpcStream([grpcOrderValue("order-1", 10)]));
      const grpcOptions = yield* resolveGrpcRuntimeOptions(feed);
      const runtimeCore = yield* makeViewServerRuntimeCore(grpcViewServer, {});
      const publishFailure: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "publish unavailable",
        topic: "orders",
      };
      const failingRuntimeClient: ViewServerRuntimeClient<GrpcTopics> = {
        ...runtimeCore.client,
        publishMany: () => Effect.fail(publishFailure),
      };
      const health = makeGrpcHealth(grpcOptions);
      const ingress = yield* makeViewServerGrpcIngress(
        grpcViewServer,
        failingRuntimeClient,
        Effect.void,
        grpcOptions,
        health,
      );

      const degradedHealth = yield* readGrpcHealthOverlayNow(runtimeCore.client, health).pipe(
        Effect.repeat({
          schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
          until: (currentHealth) => grpcHealthFeed(currentHealth)?.status === "degraded",
        }),
      );

      expect(degradedHealth.status).toBe("degraded");
      expect(grpcHealthFeed(degradedHealth)?.mappingFailuresPerSecond).toBe(0);
      expect(grpcHealthFeed(degradedHealth)?.publishFailuresPerSecond).toBe(1);
      expect(grpcHealthFeed(degradedHealth)?.lastError).toBe(
        "gRPC feed ordersFeed failed: gRPC feed publish failed for ordersFeed: publish unavailable",
      );
      yield* ingress.close;
      yield* runtimeCore.close;
    }),
  );

  it.effect("resolves explicit Kafka start policies", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const topics = {
        "orders-source": localKafkaTopic({
          regions: ["local"],
          value: kafka.json(Order),
          key: kafka.stringKey(),
          viewServerTopic: "orders",
          mapping: ({ key, value }) => ({
            id: key,
            price: value.price,
          }),
        }),
      };

      const earliest = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-earliest",
          regions,
          startFrom: "earliest",
          topics,
        },
      });
      const latest = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-latest",
          regions,
          startFrom: "latest",
          topics,
        },
      });
      const committed = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-default",
          regions,
          startFrom: {
            committedConsumerGroup: "view-server-existing-group",
            fallback: "fail",
          },
          topics,
        },
      });
      const committedWithDefaultFallback = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-default-fallback",
          regions,
          startFrom: {
            committedConsumerGroup: "view-server-existing-default-fallback-group",
          },
          topics,
        },
      });

      expect({
        committed: committed.kafkaOptions?.consume,
        committedWithDefaultFallback: committedWithDefaultFallback.kafkaOptions?.consume,
        earliest: earliest.kafkaOptions?.consume,
        latest: latest.kafkaOptions?.consume,
      }).toStrictEqual({
        committed: {
          consumerGroupId: "view-server-existing-group",
          fallbackMode: "fail",
          mode: "committed",
        },
        committedWithDefaultFallback: {
          consumerGroupId: "view-server-existing-default-fallback-group",
          fallbackMode: "earliest",
          mode: "committed",
        },
        earliest: {
          consumerGroupId: "view-server-earliest",
          fallbackMode: "earliest",
          mode: "earliest",
        },
        latest: {
          consumerGroupId: "view-server-latest",
          fallbackMode: "latest",
          mode: "latest",
        },
      });
    }),
  );

  it.live("preserves dangerous Kafka runtime option keys", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const regions = nullRecord([["__proto__", Config.succeed("localhost:9092")]]);
      let kafkaOptionsSummary:
        | {
            readonly consumerGroupId: string;
            readonly regions: Readonly<Record<string, string>>;
            readonly topics: Readonly<
              Record<
                string,
                { readonly regions: ReadonlyArray<string>; readonly viewServerTopic: string }
              >
            >;
          }
        | undefined;
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dangerousTopic = localKafkaTopic({
        regions: ["__proto__"],
        value: kafka.json(Order),
        key: kafka.stringKey(),
        viewServerTopic: "orders",
        mapping: ({ key, value }) => ({
          id: key,
          price: value.price,
        }),
      });
      const topics = nullRecord([["__proto__", dangerousTopic]]);
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.void,
          }),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, options) => {
          kafkaOptionsSummary = {
            consumerGroupId: options.consumerGroupId,
            regions: options.regions,
            topics: Object.fromEntries(
              Object.entries(options.topics).map(([sourceTopic, topic]) => [
                sourceTopic,
                {
                  regions: topic.regions,
                  viewServerTopic: topic.viewServerTopic,
                },
              ]),
            ),
          };
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-dangerous-key-test-runtime",
          regions,
          topics,
        },
      });

      expect(Object.hasOwn(kafkaOptionsSummary?.regions ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(kafkaOptionsSummary?.topics ?? {}, "__proto__")).toBe(true);
      expect({
        consumerGroupId: kafkaOptionsSummary?.consumerGroupId,
        region: kafkaOptionsSummary?.regions["__proto__"],
        topicRegions: kafkaOptionsSummary?.topics["__proto__"]?.regions,
        viewServerTopic: kafkaOptionsSummary?.topics["__proto__"]?.viewServerTopic,
      }).toStrictEqual({
        consumerGroupId: "view-server-dangerous-key-test-runtime",
        region: "localhost:9092",
        topicRegions: ["__proto__"],
        viewServerTopic: "orders",
      });

      yield* runtime.close;
    }),
  );

  it.live("returns unavailable health when Kafka ingress is degraded", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, _options, health) =>
          health.regionDisconnected("local", "lost").pipe(
            Effect.as({
              close: Effect.void,
            }),
          ),
      };

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          kafka: {
            consumerGroupId: "view-server-test-degraded",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "orders",
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            const health = yield* fetchHealth(runtime.healthUrl);

            expect(health.response.status).toBe(503);
            expect(health.health.status).toBe("degraded");
            expect(health.health.engine.topics.orders.rowCount).toBe(0);
          }),
        (runtime) => runtime.close,
      );
    }),
  );

  it.live("public live client close closes the websocket server and runtime core", () =>
    Effect.gen(function* () {
      let serverCloseCount = 0;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: makeViewServerRuntimeCore,
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.sync(() => {
              serverCloseCount += 1;
            }),
          }),
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer);
      yield* runtime.liveClient.close;
      const health = yield* runtime.client.health();

      expect(serverCloseCount).toBe(1);
      expect(health.status).toBe("stopping");
    }),
  );

  it.live("run helper keeps the runtime alive until the main fiber is interrupted", () =>
    Effect.gen(function* () {
      let serverCloseCount = 0;
      const serverStarted = yield* Deferred.make<void>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: makeViewServerRuntimeCore,
        makeServer: () =>
          Deferred.succeed(serverStarted, void 0).pipe(
            Effect.as({
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              close: Effect.sync(() => {
                serverCloseCount += 1;
              }),
            }),
          ),
      };

      const fiber = yield* runViewServerRuntimeWithDependencies(dependencies, viewServer).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(serverStarted);
      yield* Effect.sleep("10 millis");
      expect(serverCloseCount).toBe(0);

      yield* Fiber.interrupt(fiber);
      expect(serverCloseCount).toBe(1);
    }),
  );

  it.live("run helper closes Kafka ingress when the main fiber is interrupted", () =>
    Effect.gen(function* () {
      let kafkaCloseCount = 0;
      let runtimeCoreClosed = false;
      let serverCloseCount = 0;
      const kafkaStarted = yield* Deferred.make<void>();
      const serverStarted = yield* Deferred.make<void>();
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    runtimeCoreClosed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () =>
          Deferred.succeed(serverStarted, undefined).pipe(
            Effect.as({
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              close: Effect.sync(() => {
                serverCloseCount += 1;
              }),
            }),
          ),
        makeKafkaIngress: () =>
          Deferred.succeed(kafkaStarted, undefined).pipe(
            Effect.as({
              close: Effect.sync(() => {
                kafkaCloseCount += 1;
              }),
            }),
          ),
      };

      const fiber = yield* runViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-test-runtime-interrupt",
          regions,
          topics: {
            "orders-source": localKafkaTopic({
              regions: ["local"],
              value: kafka.json(Order),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              mapping: ({ key, value }) => ({
                id: key,
                price: value.price,
              }),
            }),
          },
        },
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(serverStarted);
      yield* Deferred.await(kafkaStarted);
      yield* Effect.sleep("10 millis");

      expect({
        kafkaCloseCount,
        runtimeCoreClosed,
        serverCloseCount,
      }).toStrictEqual({
        kafkaCloseCount: 0,
        runtimeCoreClosed: false,
        serverCloseCount: 0,
      });

      yield* Fiber.interrupt(fiber);

      expect({
        kafkaCloseCount,
        runtimeCoreClosed,
        serverCloseCount,
      }).toStrictEqual({
        kafkaCloseCount: 1,
        runtimeCoreClosed: true,
        serverCloseCount: 1,
      });
    }),
  );

  it.live("public run helper starts a launchable websocket runtime", () =>
    Effect.gen(function* () {
      const fiber = yield* runViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: 0,
      }).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.sleep("20 millis");
      yield* Fiber.interrupt(fiber);
    }),
  );

  it.live("public run helper supports default runtime options", () =>
    Effect.gen(function* () {
      const fiber = yield* runViewServerRuntime(viewServer).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Effect.sleep("20 millis");
      yield* Fiber.interrupt(fiber);
    }),
  );

  it.live("releases the runtime core when server startup fails before returning a runtime", () =>
    Effect.gen(function* () {
      let closed = false;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    closed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () => Effect.die(new Error("server startup failed")),
      };

      const startupExit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer),
      );

      expect(Exit.isFailure(startupExit)).toBe(true);
      expect(closed).toBe(true);
    }),
  );

  it.live("releases server and runtime core when Kafka ingress startup fails", () =>
    Effect.gen(function* () {
      let runtimeCoreClosed = false;
      let serverClosed = false;
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    runtimeCoreClosed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.sync(() => {
              serverClosed = true;
            }),
          }),
        makeKafkaIngress: () =>
          Effect.fail(
            new ViewServerKafkaIngressError({
              message: "Kafka ingress startup failed",
              cause: "startup failed",
            }),
          ),
      };

      const startupExit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          kafka: {
            consumerGroupId: "view-server-test-startup-failure",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "orders",
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
        }),
      );

      expect(Exit.isFailure(startupExit)).toBe(true);
      expect(serverClosed).toBe(true);
      expect(runtimeCoreClosed).toBe(true);
    }),
  );
});
