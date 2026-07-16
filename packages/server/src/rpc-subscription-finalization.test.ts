import { describe, expect, it } from "@effect/vitest";
import type { ViewServerLiveEvent } from "@effect-view-server/client";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import {
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "@effect-view-server/config";
import { Cause, Deferred, Effect, Exit, Fiber, Logger, References, Scope, Stream } from "effect";
import { makeViewServerWebSocketServer } from "./index";
import { makeViewServerRpcHandlers } from "./rpc-handlers";
import {
  createServerTestRuntime,
  makeRawRpcClient,
  makeServerTransportLifecycleProbe,
  serverTestLiveClientWithSubscribe,
  viewServer,
} from "../test-harness/server";

describe("Real View Server RPC subscription finalization", () => {
  it.live("closes transport stream counters when subscription acquisition fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      let openedStreams = 0;
      let closedStreams = 0;
      const subscribeError: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "subscription unavailable",
      };
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: {
          ...inMemory.liveClient,
          subscribeRuntime: () => Effect.fail(subscribeError),
        },
        runtime: inMemory.client,
        transport: {
          streamOpened: Effect.sync(() => {
            openedStreams += 1;
          }),
          streamClosed: Effect.sync(() => {
            closedStreams += 1;
          }),
        },
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);

      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.ignore));
      const failedEvents = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(failedEvents)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "remote",
          status: "error",
          code: "RuntimeUnavailable",
          message: "subscription unavailable",
        },
      ]);
      expect(openedStreams).toBe(1);
      expect(closedStreams).toBe(1);
      yield* subscription.close();
      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("logs typed RPC handler stream finalization close failures", () => {
    const logs: Array<{
      readonly cause: Cause.Cause<unknown>;
      readonly logLevel: unknown;
      readonly message: unknown;
    }> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push({
        cause: options.cause,
        logLevel: options.logLevel,
        message: options.message,
      });
    });
    let closeAttempts = 0;
    const closeFailure: ViewServerTransportError = {
      _tag: "ViewServerTransportError",
      code: "SubscriptionClosed",
      message: "close failed",
      topic: "orders",
      queryId: "close-failure",
    };
    return Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const closeStarted = yield* Deferred.make<void>();
      const liveClient = serverTestLiveClientWithSubscribe(inMemory.liveClient, () =>
        Effect.succeed({
          events: Stream.make({
            type: "snapshot",
            topic: "orders",
            queryId: "close-failure",
            version: 0,
            keys: ["order-1"],
            rows: [{ id: "order-1" }],
            totalRows: 1,
          } satisfies ViewServerLiveEvent<{ readonly id: string }>),
          close: () =>
            Effect.gen(function* () {
              closeAttempts += 1;
              yield* Deferred.succeed(closeStarted, undefined);
              return yield* Effect.fail(closeFailure);
            }),
        }),
      );
      const handlerScope = yield* Scope.make("parallel");
      yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient,
          runtime: inMemory.client,
        },
        handlerScope,
      );

      const stream = handlers["ViewServer.Subscribe"]({
        topic: "orders",
        query: { select: ["id"] },
      });
      const events = yield* stream.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "close-failure",
        version: 0,
        keys: ["order-1"],
        rows: [{ id: "order-1" }],
        totalRows: 1,
      });

      yield* Deferred.await(closeStarted).pipe(Effect.timeout("1 second"));
      expect(closeAttempts).toBe(1);
      expect(logs).toHaveLength(1);
      expect(logs[0]?.message).toStrictEqual(["RPC subscription close failed."]);
      expect(logs[0]?.logLevel).toBe("Warn");
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
      expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
      expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }).pipe(
      Effect.scoped,
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.live(
    "preserves RPC handler stream finalization close defects mixed with typed failures",
    () => {
      const logs: Array<{
        readonly cause: Cause.Cause<unknown>;
        readonly logLevel: unknown;
        readonly message: unknown;
      }> = [];
      const logger = Logger.make<unknown, void>((options) => {
        logs.push({
          cause: options.cause,
          logLevel: options.logLevel,
          message: options.message,
        });
      });
      let closeAttempts = 0;
      const closeFailure: ViewServerTransportError = {
        _tag: "ViewServerTransportError",
        code: "SubscriptionClosed",
        message: "close failed",
        topic: "orders",
        queryId: "close-failure",
      };
      return Effect.gen(function* () {
        const inMemory = createServerTestRuntime(viewServer);
        yield* Effect.addFinalizer(() => inMemory.close);
        const closeStarted = yield* Deferred.make<void>();
        const liveClient = serverTestLiveClientWithSubscribe(inMemory.liveClient, () =>
          Effect.succeed({
            events: Stream.make({
              type: "snapshot",
              topic: "orders",
              queryId: "close-failure",
              version: 0,
              keys: ["order-1"],
              rows: [{ id: "order-1" }],
              totalRows: 1,
            } satisfies ViewServerLiveEvent<{ readonly id: string }>),
            close: () =>
              Effect.gen(function* () {
                closeAttempts += 1;
                yield* Deferred.succeed(closeStarted, undefined);
                return yield* Effect.failCause(
                  Cause.fromReasons([
                    Cause.makeFailReason(closeFailure),
                    Cause.makeDieReason("close defect"),
                  ]),
                );
              }),
          }),
        );
        const handlerScope = yield* Scope.make("parallel");
        yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
        const handlers = makeViewServerRpcHandlers(
          viewServer,
          {
            liveClient,
            runtime: inMemory.client,
          },
          handlerScope,
        );

        const stream = handlers["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        });
        const cause = yield* stream.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.sandbox,
          Effect.flip,
        );

        yield* Deferred.await(closeStarted).pipe(Effect.timeout("1 second"));
        expect(Cause.hasDies(cause)).toBe(true);
        expect(Cause.hasFails(cause)).toBe(false);
        expect(closeAttempts).toBe(1);
        expect(logs).toHaveLength(1);
        expect(logs[0]?.message).toStrictEqual(["RPC subscription close failed."]);
        expect(logs[0]?.logLevel).toBe("Warn");
        expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
        expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
        expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
        yield* Scope.close(handlerScope, Exit.void);
        yield* inMemory.close;
      }).pipe(
        Effect.scoped,
        Effect.provide(Logger.layer([logger])),
        Effect.provideService(References.MinimumLogLevel, "Trace"),
      );
    },
  );

  it.live("does not fail RPC stream finalization when typed subscription close fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const closeStarted = yield* Deferred.make<void>();
      let closeAttempts = 0;
      const closeFailure: ViewServerTransportError = {
        _tag: "ViewServerTransportError",
        code: "SubscriptionClosed",
        message: "close failed",
        topic: "orders",
        queryId: "close-failure",
      };
      const liveClient = serverTestLiveClientWithSubscribe(inMemory.liveClient, () =>
        Effect.succeed({
          events: Stream.make({
            type: "snapshot",
            topic: "orders",
            queryId: "close-failure",
            version: 0,
            keys: ["order-1"],
            rows: [{ id: "order-1" }],
            totalRows: 1,
          } satisfies ViewServerLiveEvent<{ readonly id: string }>),
          close: () =>
            Effect.gen(function* () {
              closeAttempts += 1;
              yield* Deferred.succeed(closeStarted, undefined);
              return yield* Effect.fail(closeFailure);
            }),
        }),
      );
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient,
        runtime: inMemory.client,
      });
      yield* Effect.addFinalizer(() => server.close);
      const raw = yield* makeRawRpcClient(server.url);
      yield* Effect.addFinalizer(() => raw.close);

      const events = yield* raw.rpc["ViewServer.Subscribe"]({
        topic: "orders",
        query: { select: ["id"] },
      }).pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "close-failure",
        version: 0,
        keys: ["order-1"],
        rows: [{ id: "order-1" }],
        totalRows: 1,
      });

      yield* Deferred.await(closeStarted).pipe(Effect.timeout("1 second"));
      expect(closeAttempts).toBe(1);
      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("cleans up engine subscribers when the remote websocket disconnects", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const lifecycle = yield* makeServerTransportLifecycleProbe();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: lifecycle.transport,
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      yield* Effect.addFinalizer(() => subscription.close().pipe(Effect.ignore));
      const firstEventSeen = yield* Deferred.make<void>();
      const eventsFiber = yield* subscription.events.pipe(
        Stream.tap(() => Deferred.succeed(firstEventSeen, undefined)),
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(eventsFiber).pipe(Effect.asVoid));

      yield* Deferred.await(firstEventSeen).pipe(Effect.timeout("1 second"));
      const beforeDisconnect = yield* inMemory.client.health();
      expect(beforeDisconnect.engine.topics.orders.activeSubscriptions).toBe(1);

      yield* client.close;
      yield* lifecycle.awaitCount("closedStreams", 1);
      yield* lifecycle.awaitCount("closedClients", 1);

      const afterDisconnect = yield* inMemory.client.health();
      expect(afterDisconnect.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* Fiber.interrupt(eventsFiber);
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );
});
