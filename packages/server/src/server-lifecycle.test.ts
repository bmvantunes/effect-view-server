import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Fiber, Logger, Ref, References, Semaphore } from "effect";
import * as Socket from "effect/unstable/socket/Socket";
import { makeViewServerWebSocketServer } from "./index";
import { closeTrackedSockets, makeTrackedSocket } from "./websocket-tracking";
import {
  bearerAuth,
  createServerTestRuntime,
  openRawWebSocket,
  reserveTcpPort,
  sendMalformedWebSocketUpgrade,
  viewServer,
} from "../test-harness/server";

const makeRetryableClose = Effect.fn("ViewServerServer.test.close.retryable")(
  (close: Effect.Effect<void>) =>
    Effect.map(Ref.make(false), (completed) => {
      const lock = Semaphore.makeUnsafe(1);
      return lock.withPermit(
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            if (yield* Ref.get(completed)) {
              return;
            }
            yield* restore(close);
            yield* Ref.set(completed, true);
          }),
        ),
      );
    }),
);

describe("Real View Server lifecycle", () => {
  it.live("does not count plain HTTP GET requests as websocket clients", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const closeInMemory = yield* makeRetryableClose(inMemory.close);
      yield* Effect.addFinalizer(() => closeInMemory);
      let openedClients = 0;
      let closedClients = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.sync(() => {
            openedClients += 1;
          }),
          clientClosed: Effect.sync(() => {
            closedClients += 1;
          }),
        },
      });
      const closeServer = yield* makeRetryableClose(server.close);
      yield* Effect.addFinalizer(() => closeServer);

      const response = yield* Effect.promise(() => fetch(server.url.replace("ws://", "http://")));
      yield* Effect.promise(() => response.text());

      expect(response.ok).toBe(false);
      expect(openedClients).toBe(0);
      expect(closedClients).toBe(0);
      yield* closeServer;
      yield* closeInMemory;
    }).pipe(Effect.scoped),
  );

  it.live("does not count malformed websocket upgrades as clients", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const closeInMemory = yield* makeRetryableClose(inMemory.close);
      yield* Effect.addFinalizer(() => closeInMemory);
      let openedClients = 0;
      let closedClients = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.sync(() => {
            openedClients += 1;
          }),
          clientClosed: Effect.sync(() => {
            closedClients += 1;
          }),
        },
      });
      const closeServer = yield* makeRetryableClose(server.close);
      yield* Effect.addFinalizer(() => closeServer);

      yield* sendMalformedWebSocketUpgrade(server.url);

      expect(openedClients).toBe(0);
      expect(closedClients).toBe(0);
      yield* closeServer;
      yield* closeInMemory;
    }).pipe(Effect.scoped),
  );

  it.live("fails when the websocket server port is unavailable", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const inMemory = createServerTestRuntime(viewServer);
        const closeInMemory = yield* makeRetryableClose(inMemory.close);
        yield* Effect.addFinalizer(() => closeInMemory);
        const reservedPort = yield* reserveTcpPort();

        const startupError = yield* Effect.flip(
          makeViewServerWebSocketServer(
            viewServer,
            {
              liveClient: inMemory.liveClient,
              runtime: inMemory.client,
            },
            { host: "127.0.0.1", port: reservedPort },
          ),
        );

        expect(startupError._tag).toBe("ServeError");
        expect(startupError.cause).toHaveProperty("code", "EADDRINUSE");
        yield* closeInMemory;
      }),
    ),
  );

  it.live("closes tracked websocket clients when interrupted during the open hook", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const closeInMemory = yield* makeRetryableClose(inMemory.close);
      yield* Effect.addFinalizer(() => closeInMemory);
      let openedClients = 0;
      let closedClients = 0;
      const clientOpenedSignal = yield* Deferred.make<void>();
      const clientClosedSignal = yield* Deferred.make<void>();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.gen(function* () {
            openedClients += 1;
            yield* Deferred.succeed(clientOpenedSignal, void 0);
            return yield* Effect.never;
          }),
          clientClosed: Effect.gen(function* () {
            closedClients += 1;
            yield* Deferred.succeed(clientClosedSignal, void 0);
          }),
        },
      });
      const closeServer = yield* makeRetryableClose(server.close);
      yield* Effect.addFinalizer(() => closeServer);

      const socket = yield* openRawWebSocket(server.url);
      yield* Effect.addFinalizer(() => Effect.sync(() => socket.close()));
      yield* Deferred.await(clientOpenedSignal).pipe(Effect.timeout("1 second"));
      socket.close();
      const closeFiber = yield* closeServer.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.addFinalizer(() => Fiber.interrupt(closeFiber).pipe(Effect.asVoid));
      yield* Deferred.await(clientClosedSignal).pipe(Effect.timeout("1 second"));
      yield* Fiber.join(closeFiber);

      expect(openedClients).toBe(1);
      expect(closedClients).toBe(1);
      yield* closeInMemory;
    }).pipe(Effect.scoped),
  );

  it.live("tracks active websocket close frames and logs shutdown close failures", () => {
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

    return Effect.gen(function* () {
      const activeSocketClosers = new Set<Effect.Effect<void, unknown>>();
      const socketOpened = yield* Deferred.make<void>();
      const socketClosed = yield* Deferred.make<void>();
      const writtenChunks: Array<Uint8Array | string | Socket.CloseEvent> = [];
      const writeFailure = new Socket.SocketError({
        reason: new Socket.SocketWriteError({ cause: "write failed" }),
      });
      const successfulSocket = Socket.make({
        writer: Effect.succeed((chunk) =>
          Effect.sync(() => {
            writtenChunks.push(chunk);
          }),
        ),
        runRaw: (_handler, options) =>
          Effect.gen(function* () {
            const onOpen = options?.onOpen ?? Effect.void;
            yield* onOpen;
            yield* Deferred.succeed(socketOpened, undefined);
            return yield* Effect.never;
          }),
      });
      const trackedSocket = makeTrackedSocket(
        successfulSocket,
        Effect.void,
        Deferred.succeed(socketClosed, undefined),
        activeSocketClosers,
      );

      const socketFiber = yield* trackedSocket
        .runRaw(() => Effect.void)
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Effect.addFinalizer(() => Fiber.interrupt(socketFiber).pipe(Effect.asVoid));
      yield* Deferred.await(socketOpened).pipe(Effect.timeout("1 second"));
      expect(activeSocketClosers.size).toBe(1);

      yield* closeTrackedSockets(activeSocketClosers);
      expect(writtenChunks).toStrictEqual([
        new Socket.CloseEvent(1001, "View Server shutting down"),
      ]);

      const failingCloser = Effect.fail(writeFailure);
      activeSocketClosers.add(failingCloser);
      yield* closeTrackedSockets(activeSocketClosers);
      activeSocketClosers.delete(failingCloser);
      expect(logs).toHaveLength(1);
      expect(logs[0]?.message).toStrictEqual(["WebSocket shutdown failed."]);
      expect(logs[0]?.logLevel).toBe("Warn");
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);

      yield* Fiber.interrupt(socketFiber);
      yield* Deferred.await(socketClosed).pipe(Effect.timeout("1 second"));
      expect(activeSocketClosers.size).toBe(0);
    }).pipe(
      Effect.scoped,
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.live("does not let a stuck websocket close frame block other tracked sockets", () =>
    Effect.gen(function* () {
      const activeSocketClosers = new Set<Effect.Effect<void, unknown>>();
      const fastCloseRan = yield* Deferred.make<void>();
      const releaseStuckClose = yield* Deferred.make<void>();
      activeSocketClosers.add(Deferred.await(releaseStuckClose).pipe(Effect.uninterruptible));
      activeSocketClosers.add(Deferred.succeed(fastCloseRan, undefined));

      const closeFiber = yield* Effect.acquireRelease(
        closeTrackedSockets(activeSocketClosers).pipe(Effect.forkChild({ startImmediately: true })),
        (fiber) =>
          Deferred.succeed(releaseStuckClose, undefined).pipe(
            Effect.andThen(Fiber.interrupt(fiber)),
            Effect.asVoid,
          ),
      );
      yield* Deferred.await(fastCloseRan).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(releaseStuckClose, undefined);
      yield* Fiber.join(closeFiber).pipe(Effect.timeout("1 second"));
    }),
  );

  it.live("rejects websocket upgrades when auth validation fails", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      const closeInMemory = yield* makeRetryableClose(inMemory.close);
      yield* Effect.addFinalizer(() => closeInMemory);
      let openedClients = 0;
      let closedClients = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        auth: bearerAuth,
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
        transport: {
          clientOpened: Effect.sync(() => {
            openedClients += 1;
          }),
          clientClosed: Effect.sync(() => {
            closedClients += 1;
          }),
        },
      });
      const closeServer = yield* makeRetryableClose(server.close);
      yield* Effect.addFinalizer(() => closeServer);

      const socketError = yield* Effect.flip(openRawWebSocket(server.url));

      expect(socketError._tag).toBe("ServerTestWebSocketOpenError");
      expect(socketError.cause).toBeInstanceOf(Event);
      expect(openedClients).toBe(0);
      expect(closedClients).toBe(0);

      yield* closeServer;
      yield* closeInMemory;
    }).pipe(Effect.scoped),
  );
});
