import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Schema } from "effect";
import { HttpServerError } from "effect/unstable/http";
import { makeDefaultRuntimeDependencies, makeViewServerRuntimeWithDependencies } from "./internal";
import type { ViewServerRuntimeDependencies } from "./runtime-dependencies";

const Order = Schema.Struct({
  id: ViewServerId,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
    },
  },
});

const makeTrackedDependencies = (
  events: Array<string>,
): ViewServerRuntimeDependencies<typeof viewServer.topics> => {
  const defaults = makeDefaultRuntimeDependencies<typeof viewServer.topics>();
  return {
    makeRuntimeCore: (config, options) =>
      defaults.makeRuntimeCore(config, options).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            events.push("acquire:runtime-core");
          }),
        ),
        Effect.map((runtimeCore) => ({
          ...runtimeCore,
          close: Effect.sync(() => {
            events.push("close:runtime-core");
          }).pipe(Effect.andThen(runtimeCore.close)),
        })),
      ),
    makeServer: (_config, _input, _options) =>
      Effect.sync(() => {
        events.push("acquire:server");
        return {
          url: "ws://127.0.0.1:8080/rpc",
          healthUrl: "http://127.0.0.1:8080/health",
          metricsUrl: "http://127.0.0.1:8080/metrics",
          close: Effect.sync(() => {
            events.push("close:server");
          }),
        };
      }),
    makeTcpPublishIngress: (_config, _client, _options) =>
      Effect.sync(() => {
        events.push("acquire:tcp");
        return {
          url: "tcp://127.0.0.1:8081",
          close: Effect.sync(() => {
            events.push("close:tcp");
          }),
        };
      }),
  };
};

describe("generic runtime composition lifecycle", () => {
  it.live("acquires Runtime Core, server, and TCP ingress and closes them in reverse order", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const heartbeatStatuses: Array<string> = [];
      let dependencyUpdates = 0;
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        makeTrackedDependencies(events),
        viewServer,
        {
          host: "127.0.0.1",
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 0,
          websocketPort: 0,
          reporting: {
            heartbeatInterval: Duration.hours(1),
            dependenciesInterval: Duration.hours(1),
            onHeartbeat: (heartbeat) =>
              Effect.sync(() => {
                heartbeatStatuses.push(heartbeat.status);
              }),
            onDependenciesUpdate: () =>
              Effect.sync(() => {
                dependencyUpdates += 1;
              }),
          },
        },
      );
      yield* Effect.yieldNow;

      expect(events).toStrictEqual(["acquire:runtime-core", "acquire:server", "acquire:tcp"]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready"]);
      expect(dependencyUpdates).toBe(0);

      yield* runtime.close;
      yield* runtime.close;

      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "acquire:server",
        "acquire:tcp",
        "close:tcp",
        "close:server",
        "close:runtime-core",
      ]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready", "Stopping"]);
      expect(dependencyUpdates).toBe(0);
    }),
  );

  it.live("does not let in-flight reporting callbacks hold startup or shutdown", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const heartbeatStatuses: Array<string> = [];
      const startingInterrupted = yield* Deferred.make<void>();
      const readyStarted = yield* Deferred.make<void>();
      const readyInterrupted = yield* Deferred.make<void>();
      const dependenciesStarted = yield* Deferred.make<void>();
      const dependenciesInterrupted = yield* Deferred.make<void>();
      const stoppingInterrupted = yield* Deferred.make<void>();
      let dependencyCallbacks = 0;

      const runtime = yield* makeViewServerRuntimeWithDependencies(
        makeTrackedDependencies(events),
        viewServer,
        {
          websocketPort: 0,
          reporting: {
            heartbeatInterval: Duration.hours(1),
            dependenciesInterval: Duration.millis(1),
            onHeartbeat: (heartbeat) => {
              heartbeatStatuses.push(heartbeat.status);
              if (heartbeat.status === "Starting") {
                return Effect.never.pipe(
                  Effect.ensuring(Deferred.succeed(startingInterrupted, undefined)),
                );
              }
              if (heartbeat.status === "Ready") {
                return Deferred.succeed(readyStarted, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(Deferred.succeed(readyInterrupted, undefined)),
                );
              }
              return Effect.never.pipe(
                Effect.ensuring(Deferred.succeed(stoppingInterrupted, undefined)),
              );
            },
            onDependenciesUpdate: () => {
              dependencyCallbacks += 1;
              return Deferred.succeed(dependenciesStarted, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(dependenciesInterrupted, undefined)),
              );
            },
          },
        },
      );
      yield* Deferred.await(startingInterrupted);
      yield* Deferred.await(readyStarted);
      yield* Deferred.await(dependenciesStarted);

      yield* runtime.close;
      yield* Deferred.await(readyInterrupted);
      yield* Deferred.await(dependenciesInterrupted);
      yield* Deferred.await(stoppingInterrupted);

      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "acquire:server",
        "close:server",
        "close:runtime-core",
      ]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready", "Stopping"]);
      expect(dependencyCallbacks).toBe(1);
    }),
  );

  it.live("closes reporting ownership when runtime construction is interrupted", () =>
    Effect.gen(function* () {
      const heartbeatStatuses: Array<string> = [];
      const startingStarted = yield* Deferred.make<void>();
      const startingInterrupted = yield* Deferred.make<void>();
      const runtimeCoreAcquiring = yield* Deferred.make<void>();
      const runtimeCoreInterrupted = yield* Deferred.make<void>();
      const tracked = makeTrackedDependencies([]);
      const dependencies = {
        ...tracked,
        makeRuntimeCore: () =>
          Deferred.succeed(runtimeCoreAcquiring, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(runtimeCoreInterrupted, undefined)),
          ),
      } satisfies ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const startup = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        websocketPort: 0,
        reporting: {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          onHeartbeat: (heartbeat) => {
            heartbeatStatuses.push(heartbeat.status);
            return heartbeat.status === "Starting"
              ? Deferred.succeed(startingStarted, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(Deferred.succeed(startingInterrupted, undefined)),
                )
              : Effect.void;
          },
          onDependenciesUpdate: () => Effect.void,
        },
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(startingStarted);
      yield* Deferred.await(runtimeCoreAcquiring);

      yield* Fiber.interrupt(startup);
      const exit = yield* Fiber.await(startup);

      expect(
        Exit.match(exit, {
          onFailure: Cause.hasInterruptsOnly,
          onSuccess: () => false,
        }),
      ).toBe(true);
      yield* Deferred.await(startingInterrupted);
      yield* Deferred.await(runtimeCoreInterrupted);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Stopping"]);
    }),
  );

  it.live("continues the cached shutdown workflow when its first caller is interrupted", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const watcherFinalizing = yield* Deferred.make<void>();
      const allowWatcherFinalization = yield* Deferred.make<void>();
      const tracked = makeTrackedDependencies(events);
      const dependencies = {
        ...tracked,
        makeRuntimeCore: (config, options) =>
          tracked.makeRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              fatal: runtimeCore.fatal.pipe(
                Effect.ensuring(
                  Deferred.succeed(watcherFinalizing, undefined).pipe(
                    Effect.andThen(Deferred.await(allowWatcherFinalization)),
                  ),
                ),
              ),
            })),
          ),
      } satisfies ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        host: "127.0.0.1",
        tcpPublishHost: "127.0.0.1",
        tcpPublishPort: 0,
        websocketPort: 0,
      });
      const closeFiber = yield* runtime.close.pipe(Effect.forkDetach({ startImmediately: true }));
      yield* Deferred.await(watcherFinalizing);

      closeFiber.interruptUnsafe();
      const interruptedClose = yield* Fiber.await(closeFiber);

      expect(Exit.isFailure(interruptedClose)).toBe(true);
      expect(events).toStrictEqual(["acquire:runtime-core", "acquire:server", "acquire:tcp"]);

      yield* Deferred.succeed(allowWatcherFinalization, undefined);
      yield* runtime.close;

      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "acquire:server",
        "acquire:tcp",
        "close:tcp",
        "close:server",
        "close:runtime-core",
      ]);
    }),
  );

  it.live("releases Runtime Core when server acquisition fails", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const heartbeatStatuses: Array<string> = [];
      const tracked = makeTrackedDependencies(events);
      const serverFailure = new HttpServerError.ServeError({
        cause: "server acquisition failed",
      });
      const dependencies = {
        ...tracked,
        makeServer: () => Effect.fail(serverFailure),
      } satisfies ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const failure = yield* Effect.flip(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          host: "127.0.0.1",
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 0,
          websocketPort: 0,
          reporting: {
            heartbeatInterval: Duration.hours(1),
            dependenciesInterval: Duration.hours(1),
            onHeartbeat: (heartbeat) =>
              Effect.sync(() => {
                heartbeatStatuses.push(heartbeat.status);
              }),
            onDependenciesUpdate: () => Effect.void,
          },
        }),
      );

      expect(failure).toStrictEqual(serverFailure);
      expect(events).toStrictEqual(["acquire:runtime-core", "close:runtime-core"]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Stopping"]);
    }),
  );

  it.live("fails startup when Runtime Core becomes fatal during server acquisition", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const heartbeatStatuses: Array<string> = [];
      const fatalSignal = yield* Deferred.make<{
        readonly _tag: "ViewServerRuntimeError";
        readonly code: "RuntimeUnavailable";
        readonly topic: "orders";
        readonly message: "fatal during startup";
      }>();
      const serverAcquiring = yield* Deferred.make<void>();
      const tracked = makeTrackedDependencies(events);
      const fatal = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "fatal during startup",
      } as const;
      const dependencies = {
        ...tracked,
        makeRuntimeCore: (config, options) =>
          tracked.makeRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              fatal: Deferred.await(fatalSignal).pipe(Effect.flatMap(Effect.fail)),
            })),
          ),
        makeServer: () =>
          Deferred.succeed(serverAcquiring, undefined).pipe(
            Effect.andThen(
              Effect.never.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    events.push("interrupt:server-acquisition");
                  }),
                ),
              ),
            ),
          ),
      } satisfies ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const startup = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        websocketPort: 0,
        reporting: {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          onHeartbeat: (heartbeat) =>
            Effect.sync(() => {
              heartbeatStatuses.push(heartbeat.status);
            }),
          onDependenciesUpdate: () => Effect.void,
        },
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(serverAcquiring);
      yield* Deferred.succeed(fatalSignal, fatal);

      expect(yield* Effect.flip(Fiber.join(startup))).toStrictEqual(fatal);
      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "interrupt:server-acquisition",
        "close:runtime-core",
      ]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Stopping"]);
    }),
  );

  it.live("closes every runtime resource when the Stopping callback interrupts itself", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const heartbeatStatuses: Array<string> = [];
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        makeTrackedDependencies(events),
        viewServer,
        {
          tcpPublishPort: 0,
          websocketPort: 0,
          reporting: {
            heartbeatInterval: Duration.hours(1),
            dependenciesInterval: Duration.hours(1),
            onHeartbeat: (heartbeat) =>
              Effect.sync(() => {
                heartbeatStatuses.push(heartbeat.status);
              }).pipe(
                Effect.andThen(heartbeat.status === "Stopping" ? Effect.interrupt : Effect.void),
              ),
            onDependenciesUpdate: () => Effect.void,
          },
        },
      );
      yield* Effect.yieldNow;

      const closeExit = yield* runtime.close.pipe(Effect.exit);

      expect(Exit.isSuccess(closeExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "acquire:server",
        "acquire:tcp",
        "close:tcp",
        "close:server",
        "close:runtime-core",
      ]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready", "Stopping"]);
    }),
  );

  it.live("closes every runtime resource after fatal failure when Stopping interrupts itself", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const heartbeatStatuses: Array<string> = [];
      const fatal = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "fatal with interrupting reporter",
      } as const;
      const fatalSignal = yield* Deferred.make<typeof fatal>();
      const runtimeCoreClosed = yield* Deferred.make<void>();
      const tracked = makeTrackedDependencies(events);
      const dependencies = {
        ...tracked,
        makeRuntimeCore: (config, options) =>
          tracked.makeRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              fatal: Deferred.await(fatalSignal).pipe(Effect.flatMap(Effect.fail)),
              close: runtimeCore.close.pipe(
                Effect.ensuring(Deferred.succeed(runtimeCoreClosed, undefined)),
              ),
            })),
          ),
      } satisfies ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        websocketPort: 0,
        reporting: {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          onHeartbeat: (heartbeat) =>
            Effect.sync(() => {
              heartbeatStatuses.push(heartbeat.status);
            }).pipe(
              Effect.andThen(heartbeat.status === "Stopping" ? Effect.interrupt : Effect.void),
            ),
          onDependenciesUpdate: () => Effect.void,
        },
      });
      yield* Effect.yieldNow;

      yield* Deferred.succeed(fatalSignal, fatal);
      yield* Deferred.await(runtimeCoreClosed);
      const closeExit = yield* runtime.close.pipe(Effect.exit);

      expect(Exit.isSuccess(closeExit)).toBe(true);
      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "acquire:server",
        "close:server",
        "close:runtime-core",
      ]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready", "Stopping"]);
    }),
  );

  it.live("awaits the fatal-triggered scope close from concurrent runtime.close", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const heartbeatStatuses: Array<string> = [];
      const fatal = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        topic: "orders",
        message: "fatal after startup",
      } as const;
      const fatalSignal = yield* Deferred.make<typeof fatal>();
      const serverCloseStarted = yield* Deferred.make<void>();
      const allowServerClose = yield* Deferred.make<void>();
      const tracked = makeTrackedDependencies(events);
      const dependencies = {
        ...tracked,
        makeRuntimeCore: (config, options) =>
          tracked.makeRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              fatal: Deferred.await(fatalSignal).pipe(Effect.flatMap(Effect.fail)),
            })),
          ),
        makeServer: () =>
          Effect.sync(() => {
            events.push("acquire:server");
            return {
              url: "ws://127.0.0.1:8080/rpc",
              healthUrl: "http://127.0.0.1:8080/health",
              metricsUrl: "http://127.0.0.1:8080/metrics",
              close: Deferred.succeed(serverCloseStarted, undefined).pipe(
                Effect.andThen(Deferred.await(allowServerClose)),
                Effect.andThen(
                  Effect.sync(() => {
                    events.push("close:server");
                  }),
                ),
              ),
            };
          }),
      } satisfies ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        websocketPort: 0,
        reporting: {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          onHeartbeat: (heartbeat) =>
            Effect.sync(() => {
              heartbeatStatuses.push(heartbeat.status);
            }),
          onDependenciesUpdate: () => Effect.void,
        },
      });
      yield* Effect.yieldNow;
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready"]);
      yield* Deferred.succeed(fatalSignal, fatal);
      yield* Deferred.await(serverCloseStarted);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready", "Stopping"]);
      const closeFiber = yield* runtime.close.pipe(Effect.forkChild({ startImmediately: true }));

      expect(closeFiber.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(allowServerClose, undefined);
      yield* Fiber.join(closeFiber);
      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "acquire:server",
        "close:server",
        "close:runtime-core",
      ]);
      expect(heartbeatStatuses).toStrictEqual(["Starting", "Ready", "Stopping"]);
    }),
  );

  it.live("preserves the startup error when Runtime Core cleanup defects", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
      const tracked = makeTrackedDependencies(events);
      const serverFailure = new HttpServerError.ServeError({
        cause: "server acquisition failed",
      });
      const dependencies = {
        ...tracked,
        makeRuntimeCore: (config, options) =>
          tracked.makeRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.andThen(Effect.die("runtime-core cleanup defect")),
              ),
            })),
          ),
        makeServer: () => Effect.fail(serverFailure),
      } satisfies ViewServerRuntimeDependencies<typeof viewServer.topics>;
      const failure = yield* Effect.flip(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          websocketPort: 0,
        }),
      );

      expect(failure).toStrictEqual(serverFailure);
      expect(events).toStrictEqual(["acquire:runtime-core", "close:runtime-core"]);
    }),
  );
});
