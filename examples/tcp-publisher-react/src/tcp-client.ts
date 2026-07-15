import { Deferred, Effect, Schema } from "effect";
import * as Net from "node:net";

export class TcpPublisherExampleError extends Schema.TaggedErrorClass<TcpPublisherExampleError>()(
  "TcpPublisherExampleError",
  {
    cause: Schema.optional(Schema.Unknown),
    message: Schema.String,
  },
) {}

export type TcpCommand = {
  readonly op: "publish";
  readonly topic: "orders";
  readonly row: {
    readonly id: string;
    readonly customerId: string;
    readonly status: "open";
    readonly price: number;
    readonly region: string;
    readonly updatedAt: number;
  };
};

export type InvalidTcpCommand = {
  readonly op: "publish";
  readonly topic: "orders";
  readonly row: {
    readonly customerId: string;
    readonly status: "open";
    readonly price: string;
    readonly region: string;
    readonly updatedAt: number;
  };
};

export type WriteCommandOptions = {
  readonly host?: string;
  readonly port?: number;
};

type ResolvedWriteCommandOptions = {
  readonly host: string;
  readonly port: number;
};

const defaultWriteCommandOptions: ResolvedWriteCommandOptions = {
  host: "127.0.0.1",
  port: 8081,
};

const acknowledgementTimeout = "5 seconds";

const TcpPublishResponse = Schema.Union([
  Schema.Struct({
    ok: Schema.Literal(true),
  }),
  Schema.Struct({
    ok: Schema.Literal(false),
    error: Schema.Struct({
      _tag: Schema.String,
      message: Schema.String,
      phase: Schema.optional(Schema.String),
      topic: Schema.optional(Schema.String),
    }),
  }),
]);

export type TcpPublishResponse = typeof TcpPublishResponse.Type;

const parseTcpPublishResponse = Effect.fn("TcpPublisherExample.parseResponse")(function* (
  line: string,
) {
  const parsed = yield* Effect.try({
    try: () => JSON.parse(line),
    catch: (cause) =>
      new TcpPublisherExampleError({
        cause,
        message: "Invalid TCP publish acknowledgement.",
      }),
  });
  return yield* Schema.decodeUnknownEffect(TcpPublishResponse)(parsed);
});

const tcpPublishTransportError = (cause: unknown, message = "TCP publish command failed.") =>
  new TcpPublisherExampleError({ cause, message });

const createTcpResponseLineHandler = (complete: (line: string) => void) => {
  let responseBuffer = "";
  return (chunk: Buffer) => {
    responseBuffer += chunk.toString("utf8");
    const newlineIndex = responseBuffer.indexOf("\n");
    if (newlineIndex >= 0) {
      complete(responseBuffer.slice(0, newlineIndex));
    }
  };
};

const acquireCommandSocket = Effect.fn("TcpPublisherExample.acquireCommandSocket")(function* (
  commandLine: string,
  responseLine: Deferred.Deferred<string, TcpPublisherExampleError>,
) {
  return yield* Effect.acquireRelease(
    Effect.sync(() => {
      const socket = new Net.Socket();
      const onConnect = () => {
        socket.write(commandLine);
      };
      const onData = createTcpResponseLineHandler((line) => {
        Deferred.doneUnsafe(responseLine, Effect.succeed(line));
      });
      const onError = (cause: Error) => {
        Deferred.doneUnsafe(responseLine, Effect.fail(tcpPublishTransportError(cause)));
      };
      const onClose = () => {
        Deferred.doneUnsafe(
          responseLine,
          Effect.fail(
            tcpPublishTransportError(
              undefined,
              "TCP publisher closed before sending an acknowledgement.",
            ),
          ),
        );
      };

      socket.once("close", onClose);
      socket.once("connect", onConnect);
      socket.on("data", onData);
      socket.once("error", onError);

      return { socket, onClose, onConnect, onData, onError };
    }),
    ({ socket, onClose, onConnect, onData, onError }) =>
      Effect.sync(() => {
        socket.off("close", onClose);
        socket.off("connect", onConnect);
        socket.off("data", onData);
        socket.off("error", onError);
        socket.destroy();
      }),
  );
});

export const writeCommand = Effect.fn("TcpPublisherExample.writeCommand")(function* (
  command: TcpCommand | InvalidTcpCommand,
  options?: WriteCommandOptions,
) {
  const resolvedOptions: ResolvedWriteCommandOptions = {
    host: options?.host ?? defaultWriteCommandOptions.host,
    port: options?.port ?? defaultWriteCommandOptions.port,
  };
  const commandLine = yield* Effect.try({
    try: () => `${JSON.stringify(command)}\n`,
    catch: (cause) =>
      new TcpPublisherExampleError({
        cause,
        message: "Failed to encode TCP publish command.",
      }),
  });
  const responseLine = yield* Deferred.make<string, TcpPublisherExampleError>();

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const commandSocket = yield* acquireCommandSocket(commandLine, responseLine);
      yield* Effect.try({
        try: () => {
          commandSocket.socket.connect({
            host: resolvedOptions.host,
            port: resolvedOptions.port,
          });
        },
        catch: (cause) => tcpPublishTransportError(cause),
      });
      const line = yield* Deferred.await(responseLine).pipe(
        Effect.timeoutOrElse({
          duration: acknowledgementTimeout,
          orElse: () =>
            Effect.fail(
              new TcpPublisherExampleError({
                message: "Timed out waiting for TCP publish acknowledgement.",
              }),
            ),
        }),
      );
      return yield* parseTcpPublishResponse(line);
    }),
  );
});
