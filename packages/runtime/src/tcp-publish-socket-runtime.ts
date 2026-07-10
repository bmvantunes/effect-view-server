import type { ViewServerRuntimeError, ViewServerTopicConfig } from "@effect-view-server/config";
import { ViewServerAuthError } from "@effect-view-server/server";
import {
  makeSourceOwnershipPolicy,
  type SourceOwnershipPolicy,
  type ViewServerRuntimeCoreInternalClient,
} from "@effect-view-server/runtime-core/internal";
import { Cause, Effect, Exit, FiberSet, Option, Queue, Schema, Scope } from "effect";
import * as Net from "node:net";
import {
  handleTcpPublishCommandLine,
  type TcpPublishCommandError,
  ViewServerTcpPublishIngressError,
} from "./tcp-publish-command";
import type { ViewServerTcpPublishIngressOptions } from "./tcp-publish-ingress";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

type TcpResponseSocket = {
  readonly destroyed: boolean;
  readonly off: (event: "close" | "error", listener: () => void) => unknown;
  readonly once: (event: "close" | "error", listener: () => void) => unknown;
  readonly write: (chunk: string, callback: () => void) => boolean;
};

type TcpCompletionSocket = Pick<TcpResponseSocket, "off" | "once">;

type TcpResponseState = {
  readonly closed: boolean;
};

type TcpPublishSocketState = {
  buffer: string;
  closed: boolean;
  queuedCommands: number;
  readonly queue: Queue.Queue<string>;
};

type TcpPublishServerState = {
  closed: boolean;
  readonly pendingSockets: Set<Net.Socket>;
  readonly preCommandDeadlineMs: number;
  queuedCommands: number;
  readonly sockets: Map<Net.Socket, TcpPublishSocketState>;
};

type TcpPendingSocketReservation = {
  readonly isActive: () => boolean;
  readonly release: () => void;
  readonly releaseAdmission: () => void;
};

export type TcpPublishServerFactory = (
  connectionListener: (socket: Net.Socket) => void,
) => Net.Server;

export type TcpPublishSocketServer = {
  readonly close: Effect.Effect<void>;
  readonly url: string;
};

const TcpAddress = Schema.Struct({
  address: Schema.String,
  family: Schema.String,
  port: Schema.Number,
});

const defaultMaxLineBytes = 1024 * 1024;
const defaultMaxConnections = 1024;
const defaultMaxGlobalQueuedCommands = 1024;
const defaultMaxQueuedCommands = 1024;
const acceptedSocketPreCommandDeadlineMs = 30_000;
const rejectedSocketDestroyTimeoutMs = 1_000;

const isViewServerRuntimeError = (value: TcpPublishCommandError): value is ViewServerRuntimeError =>
  value._tag === "ViewServerRuntimeError" || value._tag === "ViewServerBackpressureError";

const wireError = (cause: Cause.Cause<TcpPublishCommandError>): object => {
  const failure = Cause.findErrorOption(cause);
  if (failure._tag === "Some") {
    if (isViewServerRuntimeError(failure.value)) {
      return {
        ok: false,
        error: {
          _tag: failure.value._tag,
          code: failure.value.code,
          message: failure.value.message,
          ...(failure.value.topic === undefined ? {} : { topic: failure.value.topic }),
        },
      };
    }
    if (failure.value instanceof ViewServerAuthError) {
      return {
        ok: false,
        error: {
          _tag: failure.value._tag,
          message: failure.value.message,
          status: failure.value.status,
        },
      };
    }
    return {
      ok: false,
      error: {
        _tag: failure.value._tag,
        message: failure.value.message,
        phase: failure.value.phase,
        ...(failure.value.topic === undefined ? {} : { topic: failure.value.topic }),
      },
    };
  }
  return {
    ok: false,
    error: {
      _tag: "ViewServerTcpPublishIngressError",
      message: "TCP publish command failed with an untyped cause.",
      phase: "runtime",
    },
  };
};

const logUntypedTcpCommandCause = (
  cause: Cause.Cause<TcpPublishCommandError>,
): Effect.Effect<void> => {
  if (Cause.findErrorOption(cause)._tag === "Some") {
    return Effect.void;
  }
  return Effect.logWarning("TCP publish command failed with an untyped cause.").pipe(
    Effect.annotateLogs({ cause: Cause.pretty(cause) }),
  );
};

const wireSuccess = (): object => ({ ok: true });

const jsonLine = (value: object): string => `${JSON.stringify(value)}\n`;

const tcpErrorPayload = (error: ViewServerTcpPublishIngressError): object => ({
  ok: false,
  error: {
    _tag: error._tag,
    message: error.message,
    phase: error.phase,
    topic: error.topic,
  },
});

const tcpQueueExceededError = (maxQueuedCommands: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish command queue exceeded ${maxQueuedCommands} commands.`,
    cause: { maxQueuedCommands },
    phase: "backpressure",
  });

const tcpGlobalQueueExceededError = (
  maxGlobalQueuedCommands: number,
): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish global command queue exceeded ${maxGlobalQueuedCommands} commands.`,
    cause: { maxGlobalQueuedCommands },
    phase: "backpressure",
  });

const tcpLineExceededError = (maxLineBytes: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish command exceeded ${maxLineBytes} bytes.`,
    cause: { maxLineBytes },
    phase: "backpressure",
  });

const tcpPartialLineExceededError = (maxLineBytes: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish command exceeded ${maxLineBytes} bytes without a newline.`,
    cause: { maxLineBytes },
    phase: "backpressure",
  });

const tcpConnectionExceededError = (maxConnections: number): ViewServerTcpPublishIngressError =>
  new ViewServerTcpPublishIngressError({
    message: `TCP publish connection count exceeded ${maxConnections} sockets.`,
    cause: { maxConnections },
    phase: "backpressure",
  });

/** Bridges Node socket completion into an interruptible Effect callback. */
const waitForTcpSocketOperation = (
  socket: TcpCompletionSocket,
  operation: (complete: () => void) => void,
): Effect.Effect<void> =>
  Effect.callback<void>((resume) => {
    let settled = false;
    const settle = () => {
      if (settled) {
        return;
      }
      settled = true;
      socket.off("close", settle);
      socket.off("error", settle);
      resume(Effect.void);
    };
    socket.once("close", settle);
    socket.once("error", settle);
    operation(settle);
    return Effect.sync(() => {
      socket.off("close", settle);
      socket.off("error", settle);
    });
  });

/** @internal Package-local test hook; not exported from @effect-view-server/runtime. */
export const writeTcpJsonLine = (
  socket: TcpResponseSocket,
  state: TcpResponseState,
  value: object,
): Effect.Effect<void> => {
  if (state.closed || socket.destroyed) {
    return Effect.void;
  }
  return waitForTcpSocketOperation(socket, (complete) => {
    socket.write(jsonLine(value), complete);
  });
};

const endTcpJsonLine = (socket: Net.Socket, value: object): Effect.Effect<void> =>
  waitForTcpSocketOperation(socket, (complete) => {
    socket.end(jsonLine(value), complete);
  }).pipe(
    Effect.timeoutOption(rejectedSocketDestroyTimeoutMs),
    Effect.asVoid,
    Effect.ensuring(Effect.sync(() => socket.destroy())),
  );

const rejectTcpSocket = Effect.fn("ViewServerRuntime.tcpPublish.socket.reject")(function* (
  socket: Net.Socket,
  error: ViewServerTcpPublishIngressError,
) {
  return yield* endTcpJsonLine(socket, tcpErrorPayload(error));
});

const executeLine = Effect.fn("ViewServerRuntime.tcpPublish.socket.executeLine")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  sourceOwnership: SourceOwnershipPolicy,
  line: string,
) {
  const exit = yield* handleTcpPublishCommandLine(
    {
      remoteAddress: Option.fromUndefinedOr(socket.remoteAddress),
    },
    config,
    client,
    options,
    sourceOwnership,
    line,
  ).pipe(Effect.exit);
  if (Exit.isSuccess(exit)) {
    return yield* writeTcpJsonLine(socket, state, wireSuccess());
  }
  yield* logUntypedTcpCommandCause(exit.cause).pipe(
    Effect.when(Effect.sync(() => state.closed === false)),
  );
  return yield* writeTcpJsonLine(socket, state, wireError(exit.cause));
});

const runSocketWorker = Effect.fn("ViewServerRuntime.tcpPublish.socket.worker")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  serverState: TcpPublishServerState,
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  sourceOwnership: SourceOwnershipPolicy,
) {
  while (state.closed === false && serverState.closed === false) {
    const queuedLine = yield* Queue.poll(state.queue);
    const line = Option.isSome(queuedLine)
      ? queuedLine
      : yield* Queue.take(state.queue).pipe(Effect.timeoutOption(serverState.preCommandDeadlineMs));
    if (line._tag === "None") {
      socket.destroy();
      return;
    }
    yield* executeLine(socket, state, config, client, options, sourceOwnership, line.value).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          state.queuedCommands -= 1;
          serverState.queuedCommands -= 1;
        }),
      ),
    );
  }
});

const closeNodeServer = (server: Net.Server): Effect.Effect<void> =>
  Effect.callback<void>((resume) => {
    server.close(() => resume(Effect.void));
  });

const listenNodeServer = (
  server: Net.Server,
  host: string,
  port: number,
  onSteadyStateError: (cause: Error) => void,
): Effect.Effect<void, ViewServerTcpPublishIngressError> =>
  Effect.callback<void, ViewServerTcpPublishIngressError>((resume, signal) => {
    let active = true;
    const onStartupError = (cause: Error) => {
      if (signal.aborted || active === false) {
        return;
      }
      active = false;
      resume(
        Effect.fail(
          new ViewServerTcpPublishIngressError({
            message: "TCP publish server failed to listen.",
            cause,
            phase: "listen",
          }),
        ),
      );
    };
    server.once("error", onStartupError);
    server.listen({ host, port }, () => {
      if (signal.aborted || active === false) {
        return;
      }
      active = false;
      server.on("error", onSteadyStateError);
      server.off("error", onStartupError);
      resume(Effect.void);
    });
    return Effect.sync(() => {
      active = false;
      server.off("error", onStartupError);
    });
  });

/** @internal The sole TCP listener/socket implementation for the runtime package. */
export const makeTcpPublishSocketServer = Effect.fn(
  "ViewServerRuntime.tcpPublish.socketServer.make",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  createServer: TcpPublishServerFactory,
) {
  const serverScope = yield* Scope.make("parallel");
  const runLifecycleFiber = Effect.runForkWith(yield* Effect.context<never>());
  const sourceOwnership = makeSourceOwnershipPolicy(config);
  const state: TcpPublishServerState = {
    closed: false,
    pendingSockets: new Set(),
    preCommandDeadlineMs: acceptedSocketPreCommandDeadlineMs,
    queuedCommands: 0,
    sockets: new Map(),
  };
  const runServerFiber = yield* FiberSet.makeRuntime().pipe(Scope.provide(serverScope));

  const installAcceptedSocket = Effect.fn("ViewServerRuntime.tcpPublish.socket.accept")(function* (
    socket: Net.Socket,
    reservation: TcpPendingSocketReservation,
  ) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (state.closed || socket.destroyed || reservation.isActive() === false) {
          reservation.releaseAdmission();
          socket.destroy();
          return;
        }
        const socketScope = yield* Scope.fork(serverScope, "sequential");
        const closeSocket = (yield* Effect.cached(Scope.close(socketScope, Exit.void))).pipe(
          Effect.uninterruptible,
        );
        const queue = yield* Queue.unbounded<string>();
        const socketState: TcpPublishSocketState = {
          buffer: "",
          closed: false,
          queue,
          queuedCommands: 0,
        };
        const workScope = yield* Scope.make("sequential");
        const closeWork = (yield* Effect.cached(Scope.close(workScope, Exit.void))).pipe(
          Effect.uninterruptible,
        );
        yield* Scope.addFinalizer(
          workScope,
          Effect.sync(() => {
            state.queuedCommands -= socketState.queuedCommands;
            socketState.queuedCommands = 0;
          }),
        );
        const runSocketFiber = yield* FiberSet.makeRuntime().pipe(Scope.provide(workScope));
        const socketIsClosed = (): boolean => socketState.closed || state.closed;
        const withOpenSocket = (operation: () => void): void => {
          if (socketIsClosed()) {
            return;
          }
          operation();
        };

        const closeWithError = (error: ViewServerTcpPublishIngressError): void => {
          withOpenSocket(() => {
            socketState.closed = true;
            socket.pause();
            // Admission counts logical sessions; terminal sockets are Closed while cleanup drains.
            state.sockets.delete(socket);
            runServerFiber(
              endTcpJsonLine(socket, tcpErrorPayload(error)).pipe(
                Effect.andThen(closeWork),
                Effect.andThen(closeSocket),
              ),
            );
          });
        };

        const enqueueLine = (line: string): void => {
          withOpenSocket(() => {
            const maxQueuedCommands = options.maxQueuedCommands ?? defaultMaxQueuedCommands;
            const maxGlobalQueuedCommands =
              options.maxGlobalQueuedCommands ?? defaultMaxGlobalQueuedCommands;
            if (socketState.queuedCommands >= maxQueuedCommands) {
              closeWithError(tcpQueueExceededError(maxQueuedCommands));
              return;
            }
            if (state.queuedCommands >= maxGlobalQueuedCommands) {
              closeWithError(tcpGlobalQueueExceededError(maxGlobalQueuedCommands));
              return;
            }
            socketState.queuedCommands += 1;
            state.queuedCommands += 1;
            Queue.offerUnsafe(queue, line);
          });
        };

        const onData = (chunk: string): void => {
          withOpenSocket(() => {
            const nextBuffer = socketState.buffer + chunk;
            const maxLineBytes = options.maxLineBytes ?? defaultMaxLineBytes;
            const lines = nextBuffer.split("\n");
            const partialLine = String(lines.pop());
            for (const line of lines) {
              if (Buffer.byteLength(line, "utf8") > maxLineBytes) {
                closeWithError(tcpLineExceededError(maxLineBytes));
                return;
              }
              const trimmed = line.trim();
              if (trimmed.length > 0) {
                enqueueLine(trimmed);
              }
            }
            socketState.buffer = partialLine;
            if (Buffer.byteLength(socketState.buffer, "utf8") > maxLineBytes) {
              closeWithError(tcpPartialLineExceededError(maxLineBytes));
            }
          });
        };
        const onError = (): void => {
          socket.destroy();
        };
        const onClose = (): void => {
          socketState.closed = true;
          state.sockets.delete(socket);
          runServerFiber(closeSocket);
        };

        yield* Scope.addFinalizer(
          socketScope,
          // Signal the peer first, join command work second, then remove the last error listener.
          Effect.sync(() => {
            socketState.closed = true;
            socket.pause();
            socket.off("data", onData);
            socket.off("close", onClose);
            socket.destroy();
          }).pipe(
            Effect.andThen(closeWork),
            Effect.ensuring(
              Effect.sync(() => {
                socket.off("error", onError);
                state.pendingSockets.delete(socket);
                state.sockets.delete(socket);
              }),
            ),
          ),
        );
        const transferred = yield* Effect.sync(() => {
          if (state.closed || socket.destroyed || reservation.isActive() === false) {
            return false;
          }
          socket.setEncoding("utf8");
          if (state.closed || socket.destroyed || reservation.isActive() === false) {
            return false;
          }
          socket.on("data", onData);
          socket.on("error", onError);
          socket.on("close", onClose);
          state.sockets.set(socket, socketState);
          // Permanent ownership overlaps the provisional handlers, so no error-listener gap exists.
          reservation.release();
          return true;
        });
        if (transferred === false) {
          reservation.releaseAdmission();
          return yield* closeSocket;
        }
        runSocketFiber(
          restore(
            runSocketWorker(socket, socketState, state, config, client, options, sourceOwnership),
          ),
        );
      }),
    );
  });

  const rejectPendingSocket = Effect.fn("ViewServerRuntime.tcpPublish.socket.rejectPending")(
    function* (
      socket: Net.Socket,
      maxConnections: number,
      reservation: TcpPendingSocketReservation,
    ) {
      return yield* rejectTcpSocket(socket, tcpConnectionExceededError(maxConnections)).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            reservation.releaseAdmission();
            socket.destroy();
          }),
        ),
      );
    },
  );

  const server = createServer((socket) => {
    let admissionReleased = false;
    const releaseAdmission = (): void => {
      state.pendingSockets.delete(socket);
      admissionReleased = true;
    };
    const release = (): void => {
      releaseAdmission();
      socket.off("error", onPendingError);
      socket.off("close", onPendingClose);
    };
    const onPendingError = (): void => {
      socket.destroy();
    };
    const onPendingClose = (): void => {
      release();
      // Covers a close emitted reentrantly during the synchronous ownership transfer.
      state.sockets.delete(socket);
    };
    const reservation: TcpPendingSocketReservation = {
      isActive: () => admissionReleased === false && state.pendingSockets.has(socket),
      release,
      releaseAdmission,
    };
    socket.on("error", onPendingError);
    socket.on("close", onPendingClose);
    const maxConnections = options.maxConnections ?? defaultMaxConnections;
    // Reserve synchronously at the Node callback edge, before an Effect fiber can yield.
    state.pendingSockets.add(socket);
    if (state.closed || socket.destroyed || reservation.isActive() === false) {
      reservation.releaseAdmission();
      socket.destroy();
      return;
    }
    if (state.sockets.size + state.pendingSockets.size > maxConnections) {
      runServerFiber(rejectPendingSocket(socket, maxConnections, reservation));
      return;
    }
    runServerFiber(installAcceptedSocket(socket, reservation));
  });
  const close = (yield* Effect.cached(
    Effect.sync(() => {
      state.closed = true;
      for (const socket of state.pendingSockets) {
        socket.destroy();
      }
      state.pendingSockets.clear();
      for (const socketState of state.sockets.values()) {
        socketState.closed = true;
      }
    }).pipe(Effect.andThen(Scope.close(serverScope, Exit.void))),
  )).pipe(Effect.uninterruptible);
  const onSteadyStateError = (cause: Error): void => {
    runLifecycleFiber(
      Effect.logWarning("TCP publish server emitted an error after listen; closing ingress.").pipe(
        Effect.annotateLogs({ cause }),
        Effect.andThen(close),
      ),
    );
  };
  yield* Scope.addFinalizer(
    serverScope,
    closeNodeServer(server).pipe(
      Effect.ensuring(Effect.sync(() => server.off("error", onSteadyStateError))),
    ),
  );

  return yield* Effect.uninterruptibleMask((restore) =>
    restore(
      listenNodeServer(server, options.host ?? "127.0.0.1", options.port, onSteadyStateError).pipe(
        Effect.andThen(
          Effect.sync(() => {
            const address = Schema.decodeUnknownSync(TcpAddress)(server.address());
            return {
              url: tcpPublishUrl(address),
              close,
            } satisfies TcpPublishSocketServer;
          }),
        ),
      ),
    ).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? close : Effect.void))),
  );
});

/** @internal Package-local test hook; not exported from @effect-view-server/runtime. */
export const tcpPublishUrl = (address: {
  readonly address: string;
  readonly port: number;
}): string => {
  const host = address.address.includes(":") ? `[${address.address}]` : address.address;
  return `tcp://${host}:${address.port}`;
};
