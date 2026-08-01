import { describe, expect, it } from "@effect/vitest";
import { ViewServerId, defineViewServerConfig } from "@effect-view-server/config";
import { Deferred, Effect, Exit, Fiber, Schema } from "effect";
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
      const runtime = yield* makeViewServerRuntimeWithDependencies(
        makeTrackedDependencies(events),
        viewServer,
        {
          host: "127.0.0.1",
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 0,
          websocketPort: 0,
        },
      );

      expect(events).toStrictEqual(["acquire:runtime-core", "acquire:server", "acquire:tcp"]);

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
        }),
      );

      expect(failure).toStrictEqual(serverFailure);
      expect(events).toStrictEqual(["acquire:runtime-core", "close:runtime-core"]);
    }),
  );

  it.live("fails startup when Runtime Core becomes fatal during server acquisition", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
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
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(serverAcquiring);
      yield* Deferred.succeed(fatalSignal, fatal);

      expect(yield* Effect.flip(Fiber.join(startup))).toStrictEqual(fatal);
      expect(events).toStrictEqual([
        "acquire:runtime-core",
        "interrupt:server-acquisition",
        "close:runtime-core",
      ]);
    }),
  );

  it.live("awaits the fatal-triggered scope close from concurrent runtime.close", () =>
    Effect.gen(function* () {
      const events: Array<string> = [];
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
      });
      yield* Deferred.succeed(fatalSignal, fatal);
      yield* Deferred.await(serverCloseStarted);
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
