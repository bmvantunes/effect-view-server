import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as Net from "node:net";
import { TcpPublisherExampleError, writeCommand } from "./tcp-client";

class TcpClientTestError extends Schema.TaggedErrorClass<TcpClientTestError>()(
  "TcpClientTestError",
  {
    cause: Schema.optional(Schema.Unknown),
    message: Schema.String,
  },
) {}

const ListeningAddress = Schema.Struct({
  port: Schema.Number,
});

const closeServer = Effect.fn("TcpPublisherExample.test.closeServer")((server: Net.Server) =>
  Effect.callback<void, TcpClientTestError>((resume) => {
    server.close((cause) =>
      resume(
        cause === undefined
          ? Effect.void
          : Effect.fail(
              new TcpClientTestError({
                cause,
                message: "Failed to close TCP acknowledgement test server.",
              }),
            ),
      ),
    );
  }),
);

const makeAcknowledgementServer = Effect.fn("TcpPublisherExample.test.makeAcknowledgementServer")(
  function* () {
    const connected = yield* Deferred.make<Net.Socket>();
    const commandReceived = yield* Deferred.make<string>();
    const peerClosed = yield* Deferred.make<void>();
    const sockets = new Set<Net.Socket>();
    let peerCloseCount = 0;
    const server = yield* Effect.acquireRelease(
      Effect.callback<Net.Server, TcpClientTestError>((resume) => {
        const server = Net.createServer((socket) => {
          let commandBuffer = "";
          sockets.add(socket);
          Deferred.doneUnsafe(connected, Effect.succeed(socket));

          const onData = (chunk: Buffer) => {
            commandBuffer += chunk.toString("utf8");
            const newlineIndex = commandBuffer.indexOf("\n");
            if (newlineIndex >= 0) {
              Deferred.doneUnsafe(
                commandReceived,
                Effect.succeed(commandBuffer.slice(0, newlineIndex)),
              );
            }
          };
          socket.on("data", onData);
          socket.once("close", () => {
            socket.off("data", onData);
            sockets.delete(socket);
            peerCloseCount += 1;
            Deferred.doneUnsafe(peerClosed, Effect.void);
          });
        });
        const fail = (cause: unknown) => {
          server.removeAllListeners();
          resume(
            Effect.fail(
              new TcpClientTestError({
                cause,
                message: "Failed to start TCP acknowledgement test server.",
              }),
            ),
          );
        };
        server.once("error", fail);
        server.listen(0, "127.0.0.1", () => {
          server.off("error", fail);
          resume(Effect.succeed(server));
        });
        return Effect.sync(() => {
          server.off("error", fail);
          if (server.listening) {
            server.close();
          }
        });
      }),
      (server) =>
        Effect.sync(() => {
          for (const socket of sockets) {
            socket.destroy();
          }
        }).pipe(Effect.andThen(closeServer(server)), Effect.ignore),
    );

    return {
      activeSocketCount: () => sockets.size,
      commandReceived,
      connected,
      peerClosed,
      peerCloseCount: () => peerCloseCount,
      server,
    };
  },
);

const serverPort = Effect.fn("TcpPublisherExample.test.serverPort")(function* (server: Net.Server) {
  const address = yield* Schema.decodeUnknownEffect(ListeningAddress)(server.address());
  return address.port;
});

const writePeer = Effect.fn("TcpPublisherExample.test.writePeer")(
  (socket: Net.Socket, chunk: string) =>
    Effect.callback<void, TcpClientTestError>((resume) => {
      socket.write(chunk, (cause) =>
        resume(
          cause === undefined || cause === null
            ? Effect.void
            : Effect.fail(
                new TcpClientTestError({
                  cause,
                  message: "Failed to write TCP acknowledgement test data.",
                }),
              ),
        ),
      );
    }),
);

const endPeer = Effect.fn("TcpPublisherExample.test.endPeer")((socket: Net.Socket, chunk = "") =>
  Effect.callback<void, TcpClientTestError>((resume) => {
    socket.end(chunk, () => resume(Effect.void));
  }),
);

const reserveClosedPort = Effect.acquireUseRelease(
  Effect.callback<Net.Server, TcpClientTestError>((resume) => {
    const server = Net.createServer();
    const fail = (cause: unknown) =>
      resume(
        Effect.fail(
          new TcpClientTestError({
            cause,
            message: "Failed to reserve a closed TCP test port.",
          }),
        ),
      );
    server.once("error", fail);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", fail);
      resume(Effect.succeed(server));
    });
    return Effect.sync(() => {
      server.off("error", fail);
      if (server.listening) {
        server.close();
      }
    });
  }),
  serverPort,
  (server) => closeServer(server).pipe(Effect.ignore),
);

const command = {
  op: "publish",
  topic: "orders",
  row: {
    id: "order-tcp-client",
    customerId: "customer-1",
    status: "open",
    price: 123,
    region: "usa",
    updatedAt: 1,
  },
} satisfies Parameters<typeof writeCommand>[0];

describe("tcp publisher client", () => {
  it.live("buffers newline-delimited acknowledgements split across peer writes", () =>
    Effect.gen(function* () {
      const acknowledgement = yield* makeAcknowledgementServer();
      const port = yield* serverPort(acknowledgement.server);
      const commandFiber = yield* writeCommand(command, { port }).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const peer = yield* Deferred.await(acknowledgement.connected).pipe(
        Effect.timeout("1 second"),
      );
      const receivedCommand = yield* Deferred.await(acknowledgement.commandReceived).pipe(
        Effect.timeout("1 second"),
      );

      yield* writePeer(peer, '{"ok":');
      yield* Effect.sleep("10 millis");
      expect(commandFiber.pollUnsafe()).toBeUndefined();
      yield* writePeer(peer, "true}\n");

      const response = yield* Fiber.join(commandFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(acknowledgement.peerClosed).pipe(Effect.timeout("1 second"));
      expect(receivedCommand).toBe(JSON.stringify(command));
      expect({
        activeSocketCount: acknowledgement.activeSocketCount(),
        peerCloseCount: acknowledgement.peerCloseCount(),
        response,
      }).toStrictEqual({
        activeSocketCount: 0,
        peerCloseCount: 1,
        response: { ok: true },
      });
    }),
  );

  it.live("preserves a typed negative acknowledgement", () =>
    Effect.gen(function* () {
      const acknowledgement = yield* makeAcknowledgementServer();
      const port = yield* serverPort(acknowledgement.server);
      const commandFiber = yield* writeCommand(command, { port }).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const peer = yield* Deferred.await(acknowledgement.connected).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.await(acknowledgement.commandReceived).pipe(Effect.timeout("1 second"));
      yield* writePeer(
        peer,
        `${JSON.stringify({
          ok: false,
          error: {
            _tag: "InvalidRow",
            message: "The row is invalid.",
            phase: "row",
            topic: "orders",
          },
        })}\n`,
      );

      const response = yield* Fiber.join(commandFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(acknowledgement.peerClosed).pipe(Effect.timeout("1 second"));
      expect(response).toStrictEqual({
        ok: false,
        error: {
          _tag: "InvalidRow",
          message: "The row is invalid.",
          phase: "row",
          topic: "orders",
        },
      });
      expect({
        activeSocketCount: acknowledgement.activeSocketCount(),
        peerCloseCount: acknowledgement.peerCloseCount(),
      }).toStrictEqual({ activeSocketCount: 0, peerCloseCount: 1 });
    }),
  );

  it.live("returns a typed invalid-JSON acknowledgement failure", () =>
    Effect.gen(function* () {
      const acknowledgement = yield* makeAcknowledgementServer();
      const port = yield* serverPort(acknowledgement.server);
      const commandFiber = yield* writeCommand(command, { port }).pipe(
        Effect.flip,
        Effect.forkChild({ startImmediately: true }),
      );
      const peer = yield* Deferred.await(acknowledgement.connected).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.await(acknowledgement.commandReceived).pipe(Effect.timeout("1 second"));
      yield* writePeer(peer, "not-json\n");

      const error = yield* Fiber.join(commandFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(acknowledgement.peerClosed).pipe(Effect.timeout("1 second"));
      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("Invalid TCP publish acknowledgement.");
      expect({
        activeSocketCount: acknowledgement.activeSocketCount(),
        peerCloseCount: acknowledgement.peerCloseCount(),
      }).toStrictEqual({ activeSocketCount: 0, peerCloseCount: 1 });
    }),
  );

  it.live("returns a typed schema failure for an invalid acknowledgement shape", () =>
    Effect.gen(function* () {
      const acknowledgement = yield* makeAcknowledgementServer();
      const port = yield* serverPort(acknowledgement.server);
      const commandFiber = yield* writeCommand(command, { port }).pipe(
        Effect.flip,
        Effect.forkChild({ startImmediately: true }),
      );
      const peer = yield* Deferred.await(acknowledgement.connected).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.await(acknowledgement.commandReceived).pipe(Effect.timeout("1 second"));
      yield* writePeer(peer, '{"ok":"yes"}\n');

      const error = yield* Fiber.join(commandFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(acknowledgement.peerClosed).pipe(Effect.timeout("1 second"));
      expect(Schema.isSchemaError(error)).toBe(true);
      expect({
        activeSocketCount: acknowledgement.activeSocketCount(),
        peerCloseCount: acknowledgement.peerCloseCount(),
      }).toStrictEqual({ activeSocketCount: 0, peerCloseCount: 1 });
    }),
  );

  it.live("returns a typed failure when the peer closes before acknowledging", () =>
    Effect.gen(function* () {
      const acknowledgement = yield* makeAcknowledgementServer();
      const port = yield* serverPort(acknowledgement.server);
      const commandFiber = yield* writeCommand(command, { port }).pipe(
        Effect.flip,
        Effect.forkChild({ startImmediately: true }),
      );
      const peer = yield* Deferred.await(acknowledgement.connected).pipe(
        Effect.timeout("1 second"),
      );
      yield* Deferred.await(acknowledgement.commandReceived).pipe(Effect.timeout("1 second"));
      yield* endPeer(peer, '{"ok":');

      const error = yield* Fiber.join(commandFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(acknowledgement.peerClosed).pipe(Effect.timeout("1 second"));
      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("TCP publisher closed before sending an acknowledgement.");
      expect({
        activeSocketCount: acknowledgement.activeSocketCount(),
        peerCloseCount: acknowledgement.peerCloseCount(),
      }).toStrictEqual({ activeSocketCount: 0, peerCloseCount: 1 });
    }),
  );

  it.effect("returns a typed failure when the command cannot be encoded", () =>
    Effect.gen(function* () {
      const unserializableCommand = { ...command, unsupported: 1n };
      const error = yield* writeCommand(unserializableCommand, {
        host: undefined,
        port: undefined,
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("Failed to encode TCP publish command.");
    }),
  );

  it.live("returns a typed transport failure when connection fails", () =>
    Effect.gen(function* () {
      const port = yield* reserveClosedPort;
      const error = yield* writeCommand(command, { host: "127.0.0.1", port }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("TCP publish command failed.");
    }),
  );

  it.effect("returns a typed failure for invalid connection options", () =>
    Effect.gen(function* () {
      const error = yield* writeCommand(command, { port: -1 }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("TCP publish command failed.");
    }),
  );

  it.effect("uses Effect time to own acknowledgement timeout and cleanup", () =>
    Effect.gen(function* () {
      const acknowledgement = yield* makeAcknowledgementServer();
      const port = yield* serverPort(acknowledgement.server);
      const commandFiber = yield* writeCommand(command, { port }).pipe(
        Effect.flip,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Deferred.await(acknowledgement.connected);
      yield* Deferred.await(acknowledgement.commandReceived);
      yield* TestClock.adjust("5 seconds");
      const error = yield* Fiber.join(commandFiber);
      yield* Deferred.await(acknowledgement.peerClosed);

      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("Timed out waiting for TCP publish acknowledgement.");
      expect({
        activeSocketCount: acknowledgement.activeSocketCount(),
        peerCloseCount: acknowledgement.peerCloseCount(),
      }).toStrictEqual({ activeSocketCount: 0, peerCloseCount: 1 });

      yield* TestClock.adjust("5 seconds");
      expect(acknowledgement.peerCloseCount()).toBe(1);
    }),
  );

  it.live("destroys a withholding peer connection when the command is interrupted", () =>
    Effect.gen(function* () {
      const acknowledgement = yield* makeAcknowledgementServer();
      const port = yield* serverPort(acknowledgement.server);
      const commandFiber = yield* writeCommand(command, { port }).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Deferred.await(acknowledgement.connected).pipe(Effect.timeout("1 second"));
      const receivedCommand = yield* Deferred.await(acknowledgement.commandReceived).pipe(
        Effect.timeout("1 second"),
      );
      yield* Fiber.interrupt(commandFiber).pipe(Effect.timeout("1 second"));
      const commandExit = yield* Fiber.await(commandFiber).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(acknowledgement.peerClosed).pipe(Effect.timeout("250 millis"));

      expect(receivedCommand).toBe(JSON.stringify(command));
      expect(Exit.isFailure(commandExit) && Cause.hasInterruptsOnly(commandExit.cause)).toBe(true);
      expect({
        activeSocketCount: acknowledgement.activeSocketCount(),
        peerCloseCount: acknowledgement.peerCloseCount(),
      }).toStrictEqual({ activeSocketCount: 0, peerCloseCount: 1 });
    }),
  );
});
