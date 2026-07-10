import type { ViewServerRuntimeError, ViewServerTopicConfig } from "@effect-view-server/config";
import type { ViewServerAuth } from "@effect-view-server/server";
import { ViewServerAuthError } from "@effect-view-server/server";
import {
  makeSourceOwnershipPolicy,
  type SourceOwnershipPolicy,
  type ViewServerRuntimeCoreInternalClient,
} from "@effect-view-server/runtime-core/internal";
import { Cause, Effect, Exit, Fiber, Option, Schema } from "effect";
import * as Net from "node:net";
import {
  handleTcpPublishCommandLine,
  type TcpPublishCommandError,
  ViewServerTcpPublishIngressError,
} from "./tcp-publish-command";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export { ViewServerTcpPublishIngressError } from "./tcp-publish-command";

export type ViewServerTcpPublishIngressOptions = {
  readonly host?: string;
  readonly maxConnections?: number;
  readonly maxGlobalQueuedCommands?: number;
  readonly maxLineBytes?: number;
  readonly maxQueuedCommands?: number;
  readonly port: number;
  readonly auth?: ViewServerAuth;
};

export type ViewServerTcpPublishIngress = {
  readonly url: string;
  readonly close: Effect.Effect<void>;
};

const TcpAddress = Schema.Struct({
  address: Schema.String,
  family: Schema.String,
  port: Schema.Number,
});

type TcpPublishSocketState = {
  readonly activeFibers: Set<Fiber.Fiber<void, TcpPublishCommandError>>;
  buffer: string;
  closed: boolean;
  preCommandDeadline: ReturnType<typeof globalThis.setTimeout> | undefined;
  queuedCommands: number;
  chain: Promise<void>;
};

type TcpPublishServerState = {
  readonly activeChains: Set<Promise<void>>;
  closed: boolean;
  readonly activeFibers: Set<Fiber.Fiber<void, TcpPublishCommandError>>;
  readonly preCommandDeadlineMs: number | undefined;
  queuedCommands: number;
  readonly socketStates: Map<Net.Socket, TcpPublishSocketState>;
  readonly sockets: Set<Net.Socket>;
};

type TcpDestroyableSocket = {
  readonly destroy: () => void;
};

type TcpErrorHandlingServer = {
  readonly on: (event: "error", listener: (cause: Error) => void) => unknown;
};

type TcpPublishServerFactory = (connectionListener: (socket: Net.Socket) => void) => Net.Server;

type TcpResponseSocket = {
  readonly destroyed: boolean;
  readonly off: (event: "close" | "error", listener: () => void) => unknown;
  readonly once: (event: "close" | "error", listener: () => void) => unknown;
  readonly write: (chunk: string, callback: () => void) => boolean;
};

const defaultMaxLineBytes = 1024 * 1024;
const defaultMaxConnections = 1024;
const defaultMaxGlobalQueuedCommands = 1024;
const defaultMaxQueuedCommands = 1024;
const acceptedSocketPreCommandDeadlineMs = 30_000;
const rejectedSocketDestroyTimeoutMs = 1_000;

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

const logUntypedTcpCommandCause = (cause: Cause.Cause<TcpPublishCommandError>): Promise<void> => {
  if (Cause.findErrorOption(cause)._tag === "Some") {
    return Promise.resolve();
  }
  return Effect.runPromise(
    Effect.logWarning("TCP publish command failed with an untyped cause.").pipe(
      Effect.annotateLogs({ cause: Cause.pretty(cause) }),
    ),
  );
};

const isViewServerRuntimeError = (value: TcpPublishCommandError): value is ViewServerRuntimeError =>
  value._tag === "ViewServerRuntimeError" || value._tag === "ViewServerBackpressureError";

const wireSuccess = (): object => ({ ok: true });

const jsonLine = (value: object): string => `${JSON.stringify(value)}\n`;

const endTcpError = (
  socket: Net.Socket,
  state: TcpPublishSocketState,
  error: ViewServerTcpPublishIngressError,
): void => {
  state.closed = true;
  interruptSocketFibers(state);
  socket.setTimeout(rejectedSocketDestroyTimeoutMs);
  socket.once("timeout", socket.destroy.bind(socket));
  socket.end(jsonLine(tcpErrorPayload(error)), socket.destroy.bind(socket));
};

/** @internal Package-local test hook; not exported from @effect-view-server/runtime. */
export const writeTcpJsonLine = (
  socket: TcpResponseSocket,
  state: TcpPublishSocketState,
  value: object,
): Promise<void> => {
  if (state.closed || socket.destroyed) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    let settled = false;
    const settle = () => {
      if (settled) {
        return;
      }
      settled = true;
      socket.off("close", settle);
      socket.off("error", settle);
      resolve();
    };
    socket.once("close", settle);
    socket.once("error", settle);
    socket.write(jsonLine(value), settle);
  });
};

/** @internal Package-local test hook; not exported from @effect-view-server/runtime. */
export const rejectTcpSocketWhenClosed = (
  closed: boolean,
  socket: TcpDestroyableSocket,
): boolean => {
  if (closed) {
    socket.destroy();
    return true;
  }
  return false;
};

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

const endRejectedSocket = (
  socket: Net.Socket,
  state: TcpPublishServerState,
  error: ViewServerTcpPublishIngressError,
): void => {
  state.sockets.add(socket);
  socket.on("error", socket.destroy.bind(socket));
  socket.on("close", () => state.sockets.delete(socket));
  socket.setTimeout(rejectedSocketDestroyTimeoutMs);
  socket.once("timeout", socket.destroy.bind(socket));
  socket.end(jsonLine(tcpErrorPayload(error)), socket.destroy.bind(socket));
};

const executeLine = async <const Topics extends ViewServerRuntimeTopicDefinitions>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  serverState: TcpPublishServerState,
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  sourceOwnership: SourceOwnershipPolicy,
  line: string,
): Promise<void> => {
  try {
    if (state.closed || serverState.closed) {
      return;
    }
    const fiber = Effect.runFork(
      handleTcpPublishCommandLine(
        {
          remoteAddress: Option.fromUndefinedOr(socket.remoteAddress),
        },
        config,
        client,
        options,
        sourceOwnership,
        line,
      ),
    );
    serverState.activeFibers.add(fiber);
    state.activeFibers.add(fiber);
    const exit = await Effect.runPromise(Fiber.await(fiber));
    serverState.activeFibers.delete(fiber);
    state.activeFibers.delete(fiber);
    if (state.closed || serverState.closed) {
      return;
    }
    if (Exit.isSuccess(exit)) {
      await writeTcpJsonLine(socket, state, wireSuccess());
      return;
    }
    await logUntypedTcpCommandCause(exit.cause);
    await writeTcpJsonLine(socket, state, wireError(exit.cause));
  } finally {
    state.queuedCommands -= 1;
    serverState.queuedCommands -= 1;
    if (
      state.closed === false &&
      serverState.closed === false &&
      socket.destroyed === false &&
      state.queuedCommands === 0 &&
      state.preCommandDeadline === undefined
    ) {
      armTcpPreCommandDeadline(
        socket,
        state,
        serverState.preCommandDeadlineMs ?? acceptedSocketPreCommandDeadlineMs,
      );
    }
  }
};

/** @internal Package-local test hook; not exported from @effect-view-server/runtime. */
export const tcpPublishUrl = (address: {
  readonly address: string;
  readonly port: number;
}): string => {
  const host = address.address.includes(":") ? `[${address.address}]` : address.address;
  return `tcp://${host}:${address.port}`;
};

const enqueueLine = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  serverState: TcpPublishServerState,
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  sourceOwnership: SourceOwnershipPolicy,
  line: string,
): void => {
  const maxQueuedCommands = options.maxQueuedCommands ?? defaultMaxQueuedCommands;
  const maxGlobalQueuedCommands = options.maxGlobalQueuedCommands ?? defaultMaxGlobalQueuedCommands;
  if (state.closed || serverState.closed) {
    return;
  }
  if (state.queuedCommands >= maxQueuedCommands) {
    endTcpError(socket, state, tcpQueueExceededError(maxQueuedCommands));
    return;
  }
  if (serverState.queuedCommands >= maxGlobalQueuedCommands) {
    endTcpError(socket, state, tcpGlobalQueueExceededError(maxGlobalQueuedCommands));
    return;
  }
  state.queuedCommands += 1;
  serverState.queuedCommands += 1;
  clearTcpPreCommandDeadline(state);
  const previousChain = state.chain;
  const chain = (async () => {
    await Promise.allSettled([previousChain]);
    await Promise.allSettled([
      executeLine(socket, state, serverState, config, client, options, sourceOwnership, line),
    ]);
  })();
  state.chain = chain;
  serverState.activeChains.add(chain);
  const cleanup = () => {
    serverState.activeChains.delete(chain);
    if (state.closed) {
      serverState.socketStates.delete(socket);
    }
  };
  void chain.then(cleanup);
};

const clearTcpPreCommandDeadline = (state: TcpPublishSocketState): void => {
  if (state.preCommandDeadline !== undefined) {
    globalThis.clearTimeout(state.preCommandDeadline);
    state.preCommandDeadline = undefined;
  }
};

const armTcpPreCommandDeadline = (
  socket: Net.Socket,
  state: TcpPublishSocketState,
  deadlineMs: number,
): void => {
  clearTcpPreCommandDeadline(state);
  state.preCommandDeadline = globalThis.setTimeout(socket.destroy.bind(socket), deadlineMs);
};

const interruptSocketFibers = (state: TcpPublishSocketState): void => {
  if (state.activeFibers.size > 0) {
    Effect.runFork(Effect.forEach(state.activeFibers, Fiber.interrupt, { discard: true }));
  }
};

const installSocketHandler = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  socket: Net.Socket,
  state: TcpPublishSocketState,
  serverState: TcpPublishServerState,
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  sourceOwnership: SourceOwnershipPolicy,
): void => {
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    if (state.closed || serverState.closed) {
      return;
    }
    const nextBuffer = state.buffer + chunk;
    const maxLineBytes = options.maxLineBytes ?? defaultMaxLineBytes;
    const lines = nextBuffer.split("\n");
    const partialLine = String(lines.pop());
    for (const line of lines) {
      if (Buffer.byteLength(line, "utf8") > maxLineBytes) {
        endTcpError(socket, state, tcpLineExceededError(maxLineBytes));
        return;
      }
      const trimmed = line.trim();
      if (trimmed.length > 0) {
        enqueueLine(socket, state, serverState, config, client, options, sourceOwnership, trimmed);
      }
    }
    state.buffer = partialLine;
    if (Buffer.byteLength(state.buffer, "utf8") > maxLineBytes) {
      endTcpError(socket, state, tcpPartialLineExceededError(maxLineBytes));
    }
  });
};

const closeTcpServer = (server: Net.Server, state: TcpPublishServerState): Effect.Effect<void> =>
  Effect.gen(function* () {
    state.closed = true;
    for (const socketState of state.socketStates.values()) {
      socketState.closed = true;
    }
    const serverClosed = new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    for (const socket of state.sockets) {
      socket.destroy();
    }
    yield* Effect.forEach(state.activeFibers, Fiber.interrupt, { discard: true });
    yield* Effect.promise(() => Promise.allSettled(state.activeChains));
    yield* Effect.promise(() => serverClosed);
  });

/** @internal Package-local test hook; not exported from @effect-view-server/runtime. */
export const installTcpServerSteadyStateErrorHandler = (
  server: TcpErrorHandlingServer,
  close: Effect.Effect<void>,
): void => {
  server.on("error", (cause) => {
    Effect.runFork(
      Effect.logWarning("TCP publish server emitted an error after listen; closing ingress.").pipe(
        Effect.annotateLogs({ cause }),
        Effect.andThen(close),
      ),
    );
  });
};

const validateTcpPublishOptions = (
  options: ViewServerTcpPublishIngressOptions,
): Effect.Effect<void, ViewServerTcpPublishIngressError> => {
  if (!Number.isSafeInteger(options.port) || options.port < 0 || options.port > 65535) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish port must be a safe integer between 0 and 65535.",
        cause: options.port,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxLineBytes !== undefined &&
    (!Number.isSafeInteger(options.maxLineBytes) || options.maxLineBytes <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxLineBytes must be a positive safe integer.",
        cause: options.maxLineBytes,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxConnections !== undefined &&
    (!Number.isSafeInteger(options.maxConnections) || options.maxConnections <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxConnections must be a positive safe integer.",
        cause: options.maxConnections,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxQueuedCommands !== undefined &&
    (!Number.isSafeInteger(options.maxQueuedCommands) || options.maxQueuedCommands <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxQueuedCommands must be a positive safe integer.",
        cause: options.maxQueuedCommands,
        phase: "configuration",
      }),
    );
  }
  if (
    options.maxGlobalQueuedCommands !== undefined &&
    (!Number.isSafeInteger(options.maxGlobalQueuedCommands) || options.maxGlobalQueuedCommands <= 0)
  ) {
    return Effect.fail(
      new ViewServerTcpPublishIngressError({
        message: "TCP publish maxGlobalQueuedCommands must be a positive safe integer.",
        cause: options.maxGlobalQueuedCommands,
        phase: "configuration",
      }),
    );
  }
  return Effect.void;
};

/** @internal Package-local test hook; not exported from @effect-view-server/runtime. */
export const installTcpPublishAcceptedSocket = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  socket: Net.Socket,
  state: TcpPublishServerState,
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
): void => {
  const sourceOwnership = makeSourceOwnershipPolicy(config);
  if (rejectTcpSocketWhenClosed(state.closed, socket)) {
    return;
  }
  const maxConnections = options.maxConnections ?? defaultMaxConnections;
  if (state.sockets.size >= maxConnections) {
    endRejectedSocket(socket, state, tcpConnectionExceededError(maxConnections));
    return;
  }
  const socketState: TcpPublishSocketState = {
    activeFibers: new Set(),
    buffer: "",
    chain: Promise.resolve(),
    closed: false,
    preCommandDeadline: undefined,
    queuedCommands: 0,
  };
  state.sockets.add(socket);
  state.socketStates.set(socket, socketState);
  socket.on("error", socket.destroy.bind(socket));
  armTcpPreCommandDeadline(
    socket,
    socketState,
    state.preCommandDeadlineMs ?? acceptedSocketPreCommandDeadlineMs,
  );
  socket.on("close", () => {
    socketState.closed = true;
    clearTcpPreCommandDeadline(socketState);
    state.sockets.delete(socket);
    interruptSocketFibers(socketState);
    void socketState.chain.then(() => state.socketStates.delete(socket));
  });
  installSocketHandler(socket, socketState, state, config, client, options, sourceOwnership);
};

/** @internal Package-local test seam; not exported from @effect-view-server/runtime. */
export const makeViewServerTcpPublishIngressWithServerFactory = Effect.fn(
  "ViewServerRuntime.tcpPublish.makeWithServerFactory",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerTopicConfig<Topics>,
  client: ViewServerRuntimeCoreInternalClient<Topics>,
  options: ViewServerTcpPublishIngressOptions,
  createServer: TcpPublishServerFactory,
) {
  yield* validateTcpPublishOptions(options);
  const host = options.host ?? "127.0.0.1";
  const state: TcpPublishServerState = {
    activeChains: new Set(),
    activeFibers: new Set(),
    closed: false,
    preCommandDeadlineMs: undefined,
    queuedCommands: 0,
    socketStates: new Map(),
    sockets: new Set(),
  };
  const server = createServer((socket) => {
    installTcpPublishAcceptedSocket(socket, state, config, client, options);
  });
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const close = (yield* Effect.cached(closeTcpServer(server, state))).pipe(
        Effect.uninterruptible,
      );
      yield* restore(
        Effect.callback<void, ViewServerTcpPublishIngressError>((resume) => {
          const onStartupError = (cause: Error) => {
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
          server.listen({ host, port: options.port }, () => {
            server.off("error", onStartupError);
            installTcpServerSteadyStateErrorHandler(server, close);
            resume(Effect.void);
          });
          return close;
        }),
      );
      const address = Schema.decodeUnknownSync(TcpAddress)(server.address());
      return {
        url: tcpPublishUrl(address),
        close,
      };
    }),
  );
});

export const makeViewServerTcpPublishIngress = Effect.fn("ViewServerRuntime.tcpPublish.make")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    config: ViewServerTopicConfig<Topics>,
    client: ViewServerRuntimeCoreInternalClient<Topics>,
    options: ViewServerTcpPublishIngressOptions,
  ) {
    return yield* makeViewServerTcpPublishIngressWithServerFactory(
      config,
      client,
      options,
      (connectionListener) => Net.createServer(connectionListener),
    );
  },
);
