import { describe, expect, it } from "@effect/vitest";
import type { ColumnLiveViewEngineHealth } from "@effect-view-server/column-live-view-engine";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { makeViewServerRuntimeCoreInternal } from "@effect-view-server/runtime-core/internal";
import { Cause, Deferred, Effect, Exit, Fiber, Logger, Option, References, Stream } from "effect";
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
