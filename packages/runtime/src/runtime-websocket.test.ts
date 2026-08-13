import { describe, expect, it } from "@effect/vitest";
import type { ColumnLiveViewEngineHealth } from "@effect-view-server/column-live-view-engine";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import {
  ViewServerId,
  defineViewServerConfig,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { SourceAdapter } from "@effect-view-server/source-adapter";
import { SourceAdapterServer } from "@effect-view-server/source-adapter/server";
import {
  SourceFixture,
  type SourceFixtureTarget,
} from "@effect-view-server/source-adapter-testing";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  Option,
  Queue,
  References,
  Result,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { HttpServerError } from "effect/unstable/http";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import { makeViewServerRuntime, runViewServerRuntime } from "./index";
import { tcpPublishUrl } from "./tcp-publish-ingress";
import {
  closeTestTcpServer,
  fetchHealth,
  fetchJson,
  fetchText,
  reserveTcpPort,
  RuntimeTestFailure,
  waitForTransportHealth,
} from "../test-harness/runtime";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

import { bearerAuth, order, viewServer } from "../test-harness/runtime-config";

const healthStartedPrefix = "View Server health endpoint listening at ";
const metricsStartedPrefix = "View Server metrics endpoint listening at ";
const tcpPublishStartedPrefix = "View Server TCP publish endpoint listening at ";
const sourceTarget: SourceFixtureTarget = {
  _tag: "Materialized",
};
const SourceRow = Schema.Struct({
  id: ViewServerId,
  value: Schema.String,
});

const makeRuntimeLaunchSignals = Effect.fn("ViewServerRuntime.test.launchSignals.make")(
  function* () {
    const healthUrl = yield* Deferred.make<string>();
    const metricsUrl = yield* Deferred.make<string>();
    const tcpPublishUrl = yield* Deferred.make<string>();
    const logger = Logger.make<unknown, void>((options) => {
      const message = Array.isArray(options.message) ? options.message[0] : undefined;
      if (typeof message !== "string") {
        return;
      }
      if (message.startsWith(healthStartedPrefix)) {
        Deferred.doneUnsafe(healthUrl, Effect.succeed(message.slice(healthStartedPrefix.length)));
      }
      if (message.startsWith(metricsStartedPrefix)) {
        Deferred.doneUnsafe(metricsUrl, Effect.succeed(message.slice(metricsStartedPrefix.length)));
      }
      if (message.startsWith(tcpPublishStartedPrefix)) {
        Deferred.doneUnsafe(
          tcpPublishUrl,
          Effect.succeed(message.slice(tcpPublishStartedPrefix.length)),
        );
      }
    });
    return { healthUrl, logger, metricsUrl, tcpPublishUrl };
  },
);

const stopRuntimeLaunch = Effect.fn("ViewServerRuntime.test.launch.stop")(function* <E>(
  fiber: Fiber.Fiber<never, E>,
) {
  yield* Fiber.interrupt(fiber);
  return yield* Fiber.await(fiber);
});

const listenerPort = Effect.fn("ViewServerRuntime.test.listenerPort")(function* (url: string) {
  const parsedUrl = yield* Effect.try({
    try: () => new URL(url),
    catch: () =>
      new RuntimeTestFailure({
        message: "Runtime launch URL was not valid.",
      }),
  });
  const port = Number(parsedUrl.port);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    return yield* new RuntimeTestFailure({
      message: "Runtime launch URL did not include a valid listener port.",
    });
  }
  return port;
});

describe("Runtime WebSocket and operational endpoints", () => {
  it.live(
    "requires the nominal Source Adapter service before listening and runs the source through production composition",
    () =>
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(SourceRow);
        const config = defineViewServerConfig({
          topics: {
            sourced: {
              schema: SourceRow,
              source: fixture.materializedSource({
                label: "production-composition",
              }),
            },
          },
        });
        const defaultDependencies = makeDefaultRuntimeDependencies<typeof config.topics>();
        let serverAcquisitions = 0;
        const missingDependencies: ViewServerRuntimeDependencies<typeof config.topics> = {
          ...defaultDependencies,
          makeServer: (serverConfig, input, options) => {
            serverAcquisitions += 1;
            return defaultDependencies.makeServer(serverConfig, input, options);
          },
        };
        const missingRuntime: Effect.Effect<unknown, unknown> = Reflect.apply(
          makeViewServerRuntimeWithDependencies,
          undefined,
          [missingDependencies, config],
        );
        const missingError = yield* Effect.flip(missingRuntime);

        expect(missingError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "Source Adapter runtime service controllable-fixture is missing.",
          topic: "sourced",
        });
        expect(serverAcquisitions).toBe(0);

        const fixtureContext = yield* Layer.build(fixture.layer);
        const runtime = yield* makeViewServerRuntime(config).pipe(
          Effect.provideContext(fixtureContext),
        );
        yield* Effect.addFinalizer(() => runtime.close);
        const subscription = yield* runtime.liveClient.subscribe("sourced", {
          select: ["id", "value"],
        });
        yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.ignore));
        const eventsFiber = yield* subscription.events.pipe(
          Stream.filter((event) => event.type !== "status"),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* fixture.controls.awaitActive(sourceTarget);
        yield* fixture.controls.upsert(sourceTarget, {
          id: "source-row",
          value: "through-production-runtime",
        });
        const events = yield* Fiber.join(eventsFiber);

        expect(events.map((event) => event.type)).toStrictEqual(["snapshot", "delta"]);
        yield* subscription.close();
        yield* runtime.close;
      }).pipe(Effect.scoped),
  );

  it.live("keeps scoped adapter Layer resources alive for the public run helper lifetime", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* SourceFixture.make(SourceRow);
        const config = defineViewServerConfig({
          topics: {
            sourced: {
              schema: SourceRow,
              source: fixture.materializedSource({
                label: "run-helper-layer-lifetime",
              }),
            },
          },
        });
        const acquired = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        let resourceActive = false;
        const trackedResource = Layer.effectDiscard(
          Effect.acquireRelease(
            Effect.sync(() => {
              resourceActive = true;
            }).pipe(Effect.andThen(Deferred.succeed(acquired, undefined))),
            () =>
              Effect.sync(() => {
                resourceActive = false;
              }).pipe(Effect.andThen(Deferred.succeed(released, undefined)), Effect.asVoid),
          ),
        );
        const signals = yield* makeRuntimeLaunchSignals();
        const runtimeLayer = Layer.mergeAll(
          fixture.layer,
          trackedResource,
          Logger.layer([signals.logger]),
          Layer.succeed(References.MinimumLogLevel, "Trace"),
        );
        const fiber = yield* runViewServerRuntime(config, {
          host: "127.0.0.1",
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 0,
          websocketPort: 0,
        }).pipe(Effect.provide(runtimeLayer), Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(acquired);
        yield* fixture.controls.awaitActive(sourceTarget);
        yield* Deferred.await(signals.healthUrl);
        const settled = yield* Deferred.make<Exit.Exit<void, unknown>>();
        yield* fixture.controls.upsert(
          sourceTarget,
          {
            id: "run-helper-resource",
            value: "alive",
          },
          (applicationExit) => Deferred.succeed(settled, applicationExit).pipe(Effect.asVoid),
        );

        expect({
          applicationExit: yield* Deferred.await(settled),
          resourceActive,
          releasedBeforeStop: yield* Deferred.isDone(released),
        }).toStrictEqual({
          applicationExit: Exit.void,
          resourceActive: true,
          releasedBeforeStop: false,
        });

        yield* stopRuntimeLaunch(fiber);
        yield* Deferred.await(released);
        expect(resourceActive).toBe(false);
      }),
    ),
  );

  it.live("starts a websocket runtime with health endpoint and runtime-core mutation client", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        rpcPath: "/runtime-rpc",
        healthPath: "/runtime-health",
        metricsPath: "/runtime-metrics",
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
      const metrics = yield* fetchText(runtime.metricsUrl);
      expect(runtime.url.endsWith("/runtime-rpc")).toBe(true);
      expect(runtime.healthUrl.endsWith("/runtime-health")).toBe(true);
      expect(runtime.metricsUrl.endsWith("/runtime-metrics")).toBe(true);
      expect(health.response.status).toBe(200);
      expect(health.health.engine.topics.orders.rowCount).toBe(1);
      expect(metrics.response.status).toBe(200);
      expect(metrics.text).toContain(
        'view_server_engine_topic_rows{topic="orders",state="total"} 1',
      );

      yield* subscription.close().pipe(Effect.timeout("1 second"));
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
      expect(defaultRuntime.metricsUrl.endsWith("/metrics")).toBe(true);
      expect("subscribeRuntime" in defaultRuntime.liveClient).toBe(false);
      yield* defaultRuntime.close;

      const configuredRuntime = yield* makeViewServerRuntime(viewServer, {
        websocketPort: 0,
        tcpPublishPort: 0,
        subscriptionQueueCapacity: 1,
      });
      expect(configuredRuntime.url.endsWith("/rpc")).toBe(true);
      expect(configuredRuntime.healthUrl.endsWith("/health")).toBe(true);
      expect(configuredRuntime.metricsUrl.endsWith("/metrics")).toBe(true);
      const configuredTcpPublishUrl = yield* Effect.fromNullishOr(configuredRuntime.tcpPublishUrl);
      expect(configuredTcpPublishUrl.startsWith("tcp://")).toBe(true);
      expect([
        tcpPublishUrl({ address: "127.0.0.1", port: 1234 }),
        tcpPublishUrl({ address: "::1", port: 1234 }),
        tcpPublishUrl({ address: "::", port: 1234 }),
      ]).toStrictEqual(["tcp://127.0.0.1:1234", "tcp://[::1]:1234", "tcp://[::]:1234"]);
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
      let tcpPublishOptions:
        | Parameters<RuntimeDependencies["makeTcpPublishIngress"]>[2]
        | undefined;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) => {
          runtimeCoreOptions = options;
          return makeViewServerRuntimeCoreInternal(config, options);
        },
        makeServer: (_config, input, options) => {
          serverInput = input;
          serverOptions = options;
          return Effect.succeed({
            url: "ws://127.0.0.1:0/custom-rpc",
            healthUrl: "http://127.0.0.1:0/custom-health",
            metricsUrl: "http://127.0.0.1:0/custom-metrics",
            close: Effect.void,
          });
        },
        makeTcpPublishIngress: (_config, _client, options) => {
          tcpPublishOptions = options;
          return Effect.succeed({
            url: `tcp://${options.host ?? "127.0.0.1"}:${options.port}`,
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        auth: bearerAuth,
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
          maxMembers: 2,
          maxMembersPerGroup: 3,
          maxRetainedValueEntries: 4,
        },
        host: "0.0.0.0",
        websocketPort: 1234,
        tcpPublishHost: "127.0.0.1",
        tcpPublishMaxConnections: 9,
        tcpPublishPort: 1235,
        rpcPath: "/custom-rpc",
        healthPath: "/custom-health",
        metricsPath: "/custom-metrics",
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
        serverAuthType: typeof serverInput?.auth?.validateRequest,
        serverOptions,
        tcpPublishAuthType: typeof tcpPublishOptions?.auth?.validateRequest,
        tcpPublishOptions: {
          authType: typeof tcpPublishOptions?.auth?.validateRequest,
          host: tcpPublishOptions?.host,
          maxConnections: tcpPublishOptions?.maxConnections,
          port: tcpPublishOptions?.port,
        },
        tcpPublishUrl: runtime.tcpPublishUrl,
      }).toStrictEqual({
        runtimeCoreOptions: {
          subscriptionQueueCapacity: 7,
          groupedIncrementalAdmissionLimits: {
            maxGroups: 1,
            maxMembers: 2,
            maxMembersPerGroup: 3,
            maxRetainedValueEntries: 4,
          },
          transportHealthType: "function",
        },
        serverTransportHooks: {
          clientOpenedType: "object",
          clientClosedType: "object",
          streamOpenedType: "object",
          streamClosedType: "object",
        },
        serverAuthType: "function",
        serverOptions: {
          host: "0.0.0.0",
          port: 1234,
          path: "/custom-rpc",
          healthPath: "/custom-health",
          metricsPath: "/custom-metrics",
        },
        tcpPublishAuthType: "function",
        tcpPublishOptions: {
          authType: "function",
          host: "127.0.0.1",
          maxConnections: 9,
          port: 1235,
        },
        tcpPublishUrl: "tcp://127.0.0.1:1235",
      });
      yield* runtime.close;
      const emptyGroupedLimitsRuntime = yield* makeViewServerRuntimeWithDependencies(
        dependencies,
        viewServer,
        {
          groupedIncrementalAdmissionLimits: {},
        },
      );
      yield* emptyGroupedLimitsRuntime.close;
    }),
  );

  it.effect(
    "rejects hostile transport bags and malformed nested runtime options before acquisition",
    () =>
      Effect.gen(function* () {
        let runtimeCoreAcquisitions = 0;
        const defaults = makeDefaultRuntimeDependencies<typeof viewServer.topics>();
        const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
          ...defaults,
          makeRuntimeCore: (config, options) => {
            runtimeCoreAcquisitions += 1;
            return defaults.makeRuntimeCore(config, options);
          },
        };
        const invalidOptionsEffect = <Options extends object>(
          options: Options,
        ): Effect.Effect<unknown, unknown> =>
          Reflect.apply(makeViewServerRuntimeWithDependencies, undefined, [
            dependencies,
            viewServer,
            options,
          ]);
        const symbolOption = Symbol("transport");
        const symbolOptions = Object.defineProperty({}, symbolOption, {
          enumerable: false,
          value: true,
        });
        const kafkaError = yield* Effect.flip(invalidOptionsEffect({ kafka: {} }));
        const grpcError = yield* Effect.flip(invalidOptionsEffect({ grpc: {} }));
        const symbolError = yield* Effect.flip(invalidOptionsEffect(symbolOptions));
        const numberLimitsError = yield* Effect.flip(
          invalidOptionsEffect({ groupedIncrementalAdmissionLimits: 1 }),
        );
        const nullLimitsError = yield* Effect.flip(
          invalidOptionsEffect({ groupedIncrementalAdmissionLimits: null }),
        );
        const arrayLimitsError = yield* Effect.flip(
          invalidOptionsEffect({ groupedIncrementalAdmissionLimits: [] }),
        );
        const unknownLimitError = yield* Effect.flip(
          invalidOptionsEffect({
            groupedIncrementalAdmissionLimits: {
              maxGroupz: 1,
            },
          }),
        );
        const stringLimitError = yield* Effect.flip(
          invalidOptionsEffect({
            groupedIncrementalAdmissionLimits: { maxGroups: "1" },
          }),
        );
        const nanLimitError = yield* Effect.flip(
          invalidOptionsEffect({
            groupedIncrementalAdmissionLimits: { maxMembers: Number.NaN },
          }),
        );
        const fractionalLimitError = yield* Effect.flip(
          invalidOptionsEffect({
            groupedIncrementalAdmissionLimits: { maxMembersPerGroup: 1.5 },
          }),
        );
        const zeroLimitError = yield* Effect.flip(
          invalidOptionsEffect({
            groupedIncrementalAdmissionLimits: { maxRetainedValueEntries: 0 },
          }),
        );
        const negativeLimitError = yield* Effect.flip(
          invalidOptionsEffect({
            groupedIncrementalAdmissionLimits: { maxGroups: -1 },
          }),
        );
        const ownKeysError = yield* Effect.flip(
          invalidOptionsEffect(
            new Proxy(
              {},
              {
                ownKeys: () => {
                  throw new Error("hostile ownKeys");
                },
              },
            ),
          ),
        );
        const getterError = yield* Effect.flip(
          invalidOptionsEffect(
            Object.defineProperty({}, "host", {
              enumerable: true,
              get: () => {
                throw new Error("hostile host getter");
              },
            }),
          ),
        );
        const primitiveTrapError = yield* Effect.flip(
          invalidOptionsEffect(
            new Proxy(
              {},
              {
                ownKeys: () => {
                  throw "hostile primitive";
                },
              },
            ),
          ),
        );

        expect(kafkaError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "View Server runtime options contain unsupported property: kafka.",
        });
        expect(grpcError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "View Server runtime options contain unsupported property: grpc.",
        });
        expect(symbolError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "View Server runtime options contain unsupported property: Symbol(transport).",
        });
        const invalidLimitsError = {
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message:
            "View Server runtime option groupedIncrementalAdmissionLimits must be an object.",
        };
        expect(numberLimitsError).toStrictEqual(invalidLimitsError);
        expect(nullLimitsError).toStrictEqual(invalidLimitsError);
        expect(arrayLimitsError).toStrictEqual(invalidLimitsError);
        expect(unknownLimitError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message:
            "View Server runtime option groupedIncrementalAdmissionLimits contains unsupported property: maxGroupz.",
        });
        expect([
          stringLimitError,
          nanLimitError,
          fractionalLimitError,
          zeroLimitError,
          negativeLimitError,
        ]).toStrictEqual([
          {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message:
              "View Server runtime option groupedIncrementalAdmissionLimits.maxGroups must be a positive safe integer.",
          },
          {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message:
              "View Server runtime option groupedIncrementalAdmissionLimits.maxMembers must be a positive safe integer.",
          },
          {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message:
              "View Server runtime option groupedIncrementalAdmissionLimits.maxMembersPerGroup must be a positive safe integer.",
          },
          {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message:
              "View Server runtime option groupedIncrementalAdmissionLimits.maxRetainedValueEntries must be a positive safe integer.",
          },
          {
            _tag: "ViewServerRuntimeError",
            code: "RuntimeUnavailable",
            message:
              "View Server runtime option groupedIncrementalAdmissionLimits.maxGroups must be a positive safe integer.",
          },
        ]);
        expect(ownKeysError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "hostile ownKeys",
        });
        expect(getterError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "hostile host getter",
        });
        expect(primitiveTrapError).toStrictEqual({
          _tag: "ViewServerRuntimeError",
          code: "RuntimeUnavailable",
          message: "View Server runtime options could not be inspected.",
        });
        expect(runtimeCoreAcquisitions).toBe(0);
      }),
  );

  it.live("forwards runtime auth validation to operational HTTP endpoints", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        auth: bearerAuth,
      });

      const health = yield* fetchJson(runtime.healthUrl);
      const metrics = yield* fetchJson(runtime.metricsUrl);

      expect(health.response.status).toBe(401);
      expect(health.value).toStrictEqual({
        _tag: "ViewServerAuthError",
        message: "Missing or invalid authorization header.",
      });
      expect(metrics.response.status).toBe(401);
      expect(metrics.value).toStrictEqual({
        _tag: "ViewServerAuthError",
        message: "Missing or invalid authorization header.",
      });

      yield* runtime.close;
    }),
  );

  it.live("public run helper starts a launchable websocket runtime", () =>
    Effect.gen(function* () {
      const signals = yield* makeRuntimeLaunchSignals();
      const result = yield* Effect.acquireUseRelease(
        runViewServerRuntime(viewServer, {
          host: "127.0.0.1",
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 0,
          websocketPort: 0,
        }).pipe(
          Effect.provide(Logger.layer([signals.logger])),
          Effect.provideService(References.MinimumLogLevel, "Trace"),
          Effect.forkChild({ startImmediately: true }),
        ),
        (fiber) =>
          Effect.gen(function* () {
            const readiness = yield* Effect.raceFirst(
              Effect.gen(function* () {
                const healthUrl = yield* Deferred.await(signals.healthUrl);
                const tcpPublishUrl = yield* Deferred.await(signals.tcpPublishUrl);
                const health = yield* fetchHealth(healthUrl);
                expect({
                  status: health.response.status,
                  runtimeStatus: health.health.status,
                }).toStrictEqual({
                  status: 200,
                  runtimeStatus: "ready",
                });
                return { healthUrl, tcpPublishUrl };
              }),
              Fiber.join(fiber),
            );
            const exit = yield* stopRuntimeLaunch(fiber);
            return { ...readiness, exit };
          }),
        (fiber) => stopRuntimeLaunch(fiber).pipe(Effect.asVoid),
      );

      expect(Exit.isFailure(result.exit) && Cause.hasInterruptsOnly(result.exit.cause)).toBe(true);

      const tcpPublishPort = yield* listenerPort(result.tcpPublishUrl);
      const websocketPort = yield* listenerPort(result.healthUrl);
      yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, {
          host: "127.0.0.1",
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort,
          websocketPort,
        }),
        () => Effect.void,
        (runtime) => runtime.close,
      );
    }),
  );

  it.live(
    "every transition-defect settlement mode preserves the original cause and closes all listeners and Sources",
    () =>
      Effect.gen(function* () {
        const settlementModes = ["success", "throw", "failure", "blocked"] as const;
        yield* Effect.forEach(
          settlementModes,
          (settlementMode) =>
            Effect.gen(function* () {
              const fixture = yield* SourceFixture.make(SourceRow);
              const transitionDefect = new Error(
                `injected ${settlementMode} application transition executor defect`,
              );
              const TransitionFailure = Schema.TaggedStruct("TransitionFixtureFailure", {
                message: Schema.String,
              });
              const transitionAdapter = SourceAdapter.make({
                identity: { name: `transition-defect-${settlementMode}` },
                failure: TransitionFailure,
                materialized: {
                  applicationState: "required",
                  metrics: Schema.Struct({ transitions: Schema.Number }),
                  rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
                  definitionOptions: SourceAdapter.definitionOptions<{ readonly label: string }>(),
                },
                leased: undefined,
              });
              type TransitionState = {
                readonly transitions: number;
              };
              type TransitionCommand = {
                readonly _tag: "Throw";
              };
              const transitionState = SourceAdapterServer.applicationState({
                sweepIntervalNanos: 900_000_000_000n,
                initialState: () => Object.freeze({ transitions: 0 }),
                reduce: (_state: TransitionState, _command: TransitionCommand) => {
                  throw transitionDefect;
                },
                metrics: (state) => state,
                runDueSweep: () => Effect.void,
              });
              const emitTransitionDefect = yield* Deferred.make<Effect.Effect<void>>();
              const applicationExits: Array<Exit.Exit<void, unknown>> = [];
              let settlementCallbacks = 0;
              let returnedSettlementFinalizations = 0;
              let returnedSettlementStarts = 0;
              let transitionFinalizations = 0;
              const transitionLayer = SourceAdapterServer.make(transitionAdapter, {
                materialized: {
                  applicationState: transitionState,
                  initialLaneIds: () => ["transition-defect"],
                  acquire: (input) =>
                    Effect.gen(function* () {
                      const queue = yield* Queue.unbounded<TransitionCommand>();
                      const module = transitionState.forLifetime(
                        input.lifetimeScope,
                        input.toolkit.topic,
                      );
                      yield* Deferred.succeed(
                        emitTransitionDefect,
                        Queue.offer(queue, { _tag: "Throw" }),
                      );
                      yield* Effect.addFinalizer(() =>
                        Effect.sync(() => {
                          transitionFinalizations += 1;
                        }),
                      );
                      return SourceAdapterServer.attempt([
                        SourceAdapterServer.lane({
                          id: "transition-defect",
                          events: Stream.fromQueue(queue).pipe(
                            Stream.mapEffect((command) =>
                              Effect.gen(function* () {
                                const prepared = yield* module.prepare(command);
                                const mutation = yield* input.toolkit.delete("transition-row");
                                return yield* input.toolkit.delivery(
                                  mutation,
                                  (applicationExit) => {
                                    settlementCallbacks += 1;
                                    applicationExits.push(applicationExit);
                                    if (settlementMode === "throw") {
                                      throw new Error(
                                        "injected transition settlement callback defect",
                                      );
                                    }
                                    const started = Effect.sync(() => {
                                      returnedSettlementStarts += 1;
                                    });
                                    const returned =
                                      settlementMode === "failure"
                                        ? started.pipe(
                                            Effect.andThen(
                                              Effect.fail({
                                                _tag: "TransitionFixtureFailure" as const,
                                                message: "injected returned settlement failure",
                                              }),
                                            ),
                                          )
                                        : settlementMode === "blocked"
                                          ? started.pipe(Effect.andThen(Effect.never))
                                          : started;
                                    return returned.pipe(
                                      Effect.ensuring(
                                        Effect.sync(() => {
                                          returnedSettlementFinalizations += 1;
                                        }),
                                      ),
                                    );
                                  },
                                  prepared.transition,
                                );
                              }),
                            ),
                          ),
                        }),
                      ]);
                    }),
                  metrics: (input) =>
                    Effect.sync(() =>
                      transitionState.forLifetime(input.lifetimeScope, input.topic).metrics(),
                    ),
                  retry: Schedule.recurs(0),
                },
              });
              const sourcedConfig = defineViewServerConfig({
                topics: {
                  defective: {
                    schema: SourceRow,
                    source: transitionAdapter.materializedSource({
                      label: "root-fatal-transition",
                    }),
                  },
                  sibling: {
                    schema: SourceRow,
                    source: fixture.materializedSource({
                      label: "root-fatal-sibling",
                    }),
                  },
                },
              });
              const signals = yield* makeRuntimeLaunchSignals();
              const launchLayer = Layer.mergeAll(
                transitionLayer,
                fixture.layer,
                Logger.layer([signals.logger]),
                Layer.succeed(References.MinimumLogLevel, "Trace"),
              );
              const runtimeFiber = yield* runViewServerRuntime(sourcedConfig, {
                host: "127.0.0.1",
                websocketPort: 0,
              }).pipe(Effect.provide(launchLayer), Effect.forkChild({ startImmediately: true }));
              const healthUrl = yield* Deferred.await(signals.healthUrl);
              const port = yield* listenerPort(healthUrl);
              yield* fixture.controls.awaitActive(sourceTarget);
              yield* yield* Deferred.await(emitTransitionDefect);
              const runtimeExit = yield* Fiber.await(runtimeFiber);
              const runtimeCause = Option.getOrThrow(Exit.getCause(runtimeExit));
              const applicationCause = Option.getOrThrow(
                Exit.getCause(Option.getOrThrow(Option.fromUndefinedOr(applicationExits[0]))),
              );

              expect({
                applicationDefect: Result.getOrThrow(Cause.findDefect(applicationCause)),
                applicationExitCount: applicationExits.length,
                rootDefect: Result.getOrThrow(Cause.findDefect(runtimeCause)),
                rootFailure: Option.getOrThrow(Cause.findErrorOption(runtimeCause)),
                settlementCallbacks,
              }).toStrictEqual({
                applicationDefect: transitionDefect,
                applicationExitCount: 1,
                rootDefect: transitionDefect,
                rootFailure: {
                  _tag: "ViewServerRuntimeError",
                  code: "RuntimeUnavailable",
                  topic: "defective",
                  message: "Source application transition failed and stopped the complete runtime.",
                },
                settlementCallbacks: 1,
              });

              const replacement = yield* Effect.retry(
                makeViewServerRuntime(viewServer, {
                  host: "127.0.0.1",
                  websocketPort: port,
                }),
                Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
              );
              yield* fixture.controls.awaitCounts(sourceTarget, {
                acquisitions: 1n,
                finalizations: 1n,
              });
              expect({
                returnedSettlementFinalizations,
                returnedSettlementStarts,
                transitionFinalizations,
              }).toStrictEqual({
                returnedSettlementFinalizations: settlementMode === "throw" ? 0 : 1,
                returnedSettlementStarts: settlementMode === "throw" ? 0 : 1,
                transitionFinalizations: 1,
              });

              yield* replacement.close;
            }),
          { discard: true },
        );
      }),
  );

  it.live(
    "every maintenance-supervisor phase fault closes the listener and all Sources without retry supervision",
    () =>
      Effect.gen(function* () {
        const phases = ["sleep", "index-read", "lease-acquisition", "outcome-execution"] as const;
        yield* Effect.forEach(
          phases,
          (phase) =>
            Effect.gen(function* () {
              const fixture = yield* SourceFixture.make(SourceRow);
              const fault = new Error(`injected maintenance ${phase} defect`);
              const sweepIntervalNanos = 7_654_321n;
              const sweepTrigger = yield* Deferred.make<void>();
              const sourceActive = yield* Deferred.make<void>();
              let leaseAcquisitions = 0;
              let leaseHeld = false;
              let leaseReleases = 0;
              let scheduleSteps = 0;
              let sourceAcquisitions = 0;
              let sourceFinalizations = 0;
              const maintenanceAdapter = SourceAdapter.make({
                identity: { name: `maintenance-fatal-${phase}` },
                failure: RuntimeTestFailure,
                materialized: {
                  applicationState: "required",
                  metrics: Schema.Struct({ sweeps: Schema.Number }),
                  rejectionLocation: Schema.Struct({ offset: Schema.BigInt }),
                  definitionOptions: SourceAdapter.definitionOptions<{
                    readonly label: string;
                  }>(),
                },
                leased: undefined,
              });
              type MaintenanceState = {
                readonly sweeps: number;
              };
              type MaintenanceCommand = {
                readonly _tag: "Sweep";
              };
              const sweepCommand: MaintenanceCommand = Object.freeze({
                _tag: "Sweep",
              });
              const applicationState = SourceAdapterServer.applicationState({
                sweepIntervalNanos,
                initialState: () => Object.freeze({ sweeps: 0 }),
                reduce: (state: MaintenanceState, _command: MaintenanceCommand) =>
                  Object.freeze({
                    sweeps: state.sweeps + 1,
                  }),
                metrics: (state) => state,
                runDueSweep: (input) => {
                  if (phase === "sleep") {
                    return Effect.void;
                  }
                  const afterTrigger = Deferred.await(sweepTrigger);
                  if (phase === "index-read") {
                    return afterTrigger.pipe(Effect.andThen(Effect.die(fault)));
                  }
                  if (phase === "lease-acquisition") {
                    return afterTrigger.pipe(
                      Effect.andThen(
                        Effect.acquireUseRelease(
                          Effect.sync((): void => {
                            leaseAcquisitions += 1;
                            throw fault;
                          }),
                          () => Effect.void,
                          () =>
                            Effect.sync(() => {
                              leaseReleases += 1;
                            }),
                        ),
                      ),
                    );
                  }
                  return afterTrigger.pipe(
                    Effect.andThen(
                      Effect.acquireUseRelease(
                        Effect.sync(() => {
                          leaseAcquisitions += 1;
                          leaseHeld = true;
                        }),
                        () =>
                          input
                            .execute(
                              input.operation({
                                id: "maintenance-fatal",
                                workId: "maintenance-fatal:1",
                                isCurrent: () => {
                                  throw fault;
                                },
                                onSuccess: sweepCommand,
                                onFailure: () => sweepCommand,
                                onStale: sweepCommand,
                              }),
                            )
                            .pipe(Effect.asVoid),
                        () =>
                          Effect.sync(() => {
                            leaseHeld = false;
                            leaseReleases += 1;
                          }),
                      ),
                    ),
                  );
                },
              });
              const maintenanceLayer = SourceAdapterServer.make(maintenanceAdapter, {
                materialized: {
                  applicationState,
                  initialLaneIds: () => ["maintenance"],
                  acquire: () =>
                    Effect.gen(function* () {
                      sourceAcquisitions += 1;
                      yield* Effect.addFinalizer(() =>
                        Effect.sync(() => {
                          sourceFinalizations += 1;
                        }),
                      );
                      yield* Deferred.succeed(sourceActive, undefined);
                      return SourceAdapterServer.attempt([
                        SourceAdapterServer.lane({
                          id: "maintenance",
                          events: Stream.never,
                        }),
                      ]);
                    }),
                  metrics: (input) =>
                    Effect.sync(() =>
                      applicationState.forLifetime(input.lifetimeScope, input.topic).metrics(),
                    ),
                  retry: Schedule.recurs(10).pipe(
                    Schedule.tap(() =>
                      Effect.sync(() => {
                        scheduleSteps += 1;
                      }),
                    ),
                  ),
                },
              });
              const config = defineViewServerConfig({
                topics: {
                  defective: {
                    schema: SourceRow,
                    source: maintenanceAdapter.materializedSource({
                      label: `maintenance-${phase}`,
                    }),
                  },
                  sibling: {
                    schema: SourceRow,
                    source: fixture.materializedSource({
                      label: `maintenance-sibling-${phase}`,
                    }),
                  },
                },
              });
              const signals = yield* makeRuntimeLaunchSignals();
              const baseClock = yield* Clock.Clock;
              const maintenanceClock: Clock.Clock = {
                currentTimeMillisUnsafe: () => baseClock.currentTimeMillisUnsafe(),
                currentTimeMillis: baseClock.currentTimeMillis,
                currentTimeNanosUnsafe: () => baseClock.currentTimeNanosUnsafe(),
                currentTimeNanos: baseClock.currentTimeNanos,
                monotonicTimeNanosUnsafe: () => baseClock.monotonicTimeNanosUnsafe(),
                monotonicTimeNanos: baseClock.monotonicTimeNanos,
                sleep: (duration) =>
                  phase === "sleep" && Duration.toNanosUnsafe(duration) === sweepIntervalNanos
                    ? Deferred.await(sweepTrigger).pipe(Effect.andThen(Effect.die(fault)))
                    : baseClock.sleep(duration),
              };
              const runtimeFiber = yield* runViewServerRuntime(config, {
                host: "127.0.0.1",
                websocketPort: 0,
              }).pipe(
                Effect.provide(
                  Layer.mergeAll(
                    maintenanceLayer,
                    fixture.layer,
                    Logger.layer([signals.logger]),
                    Layer.succeed(References.MinimumLogLevel, "Trace"),
                  ),
                ),
                Effect.provideService(Clock.Clock, maintenanceClock),
                Effect.forkChild({ startImmediately: true }),
              );
              const healthUrl = yield* Deferred.await(signals.healthUrl);
              const port = yield* listenerPort(healthUrl);
              yield* Deferred.await(sourceActive);
              yield* fixture.controls.awaitActive(sourceTarget);
              yield* Deferred.succeed(sweepTrigger, undefined);
              const runtimeExit = yield* Fiber.await(runtimeFiber);
              const runtimeCause = Option.getOrThrow(Exit.getCause(runtimeExit));
              const expectedMessage =
                phase === "outcome-execution"
                  ? "Source maintenance state validation failed fatally and stopped the complete runtime."
                  : "Source maintenance supervisor failed fatally and closed the complete runtime.";

              expect({
                defect: Result.getOrThrow(Cause.findDefect(runtimeCause)),
                failure: Option.getOrThrow(Cause.findErrorOption(runtimeCause)),
                leaseAcquisitions,
                leaseHeld,
                leaseReleases,
                scheduleSteps,
                sourceAcquisitions,
                sourceFinalizations,
              }).toStrictEqual({
                defect: fault,
                failure: {
                  _tag: "ViewServerRuntimeError",
                  code: "RuntimeUnavailable",
                  topic: "defective",
                  message: expectedMessage,
                },
                leaseAcquisitions:
                  phase === "lease-acquisition" || phase === "outcome-execution" ? 1 : 0,
                leaseHeld: false,
                leaseReleases: phase === "outcome-execution" ? 1 : 0,
                scheduleSteps: 0,
                sourceAcquisitions: 1,
                sourceFinalizations: 1,
              });

              const replacement = yield* Effect.retry(
                makeViewServerRuntime(viewServer, {
                  host: "127.0.0.1",
                  websocketPort: port,
                }),
                Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
              );
              yield* fixture.controls.awaitCounts(sourceTarget, {
                acquisitions: 1n,
                finalizations: 1n,
              });
              yield* replacement.close;
            }),
          { discard: true },
        );
      }),
  );

  it.live("direct runtime factory closes for pure defect and interrupt fatal causes", () =>
    Effect.gen(function* () {
      const fatalCases = [
        {
          label: "pure-defect",
          cause: Cause.die(new Error("pure fatal defect")),
        },
        {
          label: "pure-interrupt",
          cause: Cause.interrupt(917),
        },
      ];
      for (const fatalCase of fatalCases) {
        const fixture = yield* SourceFixture.make(SourceRow);
        const sourcedConfig = defineViewServerConfig({
          topics: {
            sourced: {
              schema: SourceRow,
              source: fixture.materializedSource({
                label: fatalCase.label,
              }),
            },
          },
        });
        const fatalSignal = yield* Deferred.make<Cause.Cause<ViewServerRuntimeError>>();
        const defaults = makeDefaultRuntimeDependencies<typeof sourcedConfig.topics>();
        const dependencies: ViewServerRuntimeDependencies<typeof sourcedConfig.topics> = {
          ...defaults,
          makeRuntimeCore: (config, options) =>
            defaults.makeRuntimeCore(config, options).pipe(
              Effect.map((runtimeCore) => ({
                ...runtimeCore,
                fatal: Deferred.await(fatalSignal).pipe(Effect.flatMap(Effect.failCause)),
              })),
            ),
        };
        const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, sourcedConfig, {
          host: "127.0.0.1",
          websocketPort: 0,
        }).pipe(Effect.provide(fixture.layer));
        const port = yield* listenerPort(runtime.healthUrl);
        yield* fixture.controls.awaitActive(sourceTarget);
        yield* Deferred.succeed(fatalSignal, fatalCase.cause);

        const replacement = yield* Effect.retry(
          makeViewServerRuntime(viewServer, {
            host: "127.0.0.1",
            websocketPort: port,
          }),
          Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
        );
        yield* fixture.controls.awaitCounts(sourceTarget, {
          acquisitions: 1n,
          finalizations: 1n,
        });

        yield* replacement.close;
        yield* runtime.close;
      }
    }),
  );

  it.live("public run helper supports default runtime options", () =>
    Effect.gen(function* () {
      const signals = yield* makeRuntimeLaunchSignals();
      const result = yield* Effect.acquireUseRelease(
        runViewServerRuntime(viewServer).pipe(
          Effect.provide(Logger.layer([signals.logger])),
          Effect.provideService(References.MinimumLogLevel, "Trace"),
          Effect.forkChild({ startImmediately: true }),
        ),
        (fiber) =>
          Effect.gen(function* () {
            const readiness = yield* Effect.raceFirst(
              Effect.gen(function* () {
                const healthUrl = yield* Deferred.await(signals.healthUrl);
                const metricsUrl = yield* Deferred.await(signals.metricsUrl);
                const health = yield* fetchHealth(healthUrl);
                expect({
                  status: health.response.status,
                  runtimeStatus: health.health.status,
                }).toStrictEqual({
                  status: 200,
                  runtimeStatus: "ready",
                });
                return { healthUrl, metricsUrl };
              }),
              Fiber.join(fiber),
            );
            const exit = yield* stopRuntimeLaunch(fiber);
            return { ...readiness, exit };
          }),
        (fiber) => stopRuntimeLaunch(fiber).pipe(Effect.asVoid),
      );

      expect(Exit.isFailure(result.exit) && Cause.hasInterruptsOnly(result.exit.cause)).toBe(true);

      const websocketPort = yield* listenerPort(result.healthUrl);
      yield* Effect.acquireUseRelease(
        makeViewServerRuntime(viewServer, {
          host: "127.0.0.1",
          websocketPort,
        }),
        () => Effect.void,
        (runtime) => runtime.close,
      );
    }),
  );

  it.live("public run helper reports an occupied websocket listener", () =>
    Effect.acquireUseRelease(
      reserveTcpPort(),
      (reserved) =>
        Effect.gen(function* () {
          const exit = yield* Effect.acquireUseRelease(
            runViewServerRuntime(viewServer, {
              host: "127.0.0.1",
              websocketPort: reserved.port,
            }).pipe(Effect.forkChild({ startImmediately: true })),
            (fiber) => Fiber.await(fiber).pipe(Effect.timeout("10 seconds")),
            (fiber) => stopRuntimeLaunch(fiber).pipe(Effect.asVoid),
          );
          const error = Exit.isFailure(exit)
            ? Option.getOrUndefined(Cause.findErrorOption(exit.cause))
            : undefined;
          const lowLevelCauseCode =
            error instanceof HttpServerError.ServeError &&
            typeof error.cause === "object" &&
            error.cause !== null &&
            "code" in error.cause
              ? error.cause.code
              : undefined;
          expect(Exit.isFailure(exit)).toBe(true);
          expect(error).toBeInstanceOf(HttpServerError.ServeError);
          expect(lowLevelCauseCode).toBe("EADDRINUSE");
        }),
      ({ server }) => closeTestTcpServer(server),
    ),
  );
});
