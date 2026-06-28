import { Effect, Schema } from "effect";
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

export const writeCommand = (command: TcpCommand | InvalidTcpCommand) =>
  Effect.tryPromise({
    try: () =>
      new Promise<unknown>((resolve, reject) => {
        let isSettled = false;
        const socket = Net.createConnection({ host: "127.0.0.1", port: 8081 }, () => {
          socket.write(`${JSON.stringify(command)}\n`);
        });
        const finish = (chunk: Buffer) => {
          if (!isSettled) {
            isSettled = true;
            socket.end();
            resolve(JSON.parse(chunk.toString("utf8")));
          }
        };
        const fail = (cause: unknown) => {
          if (!isSettled) {
            isSettled = true;
            socket.destroy();
            reject(cause);
          }
        };
        socket.setTimeout(5_000, () =>
          fail(
            new TcpPublisherExampleError({
              message: "Timed out waiting for TCP publish acknowledgement.",
            }),
          ),
        );
        socket.once("error", fail);
        socket.once("data", finish);
      }),
    catch: (cause) =>
      cause instanceof TcpPublisherExampleError
        ? cause
        : new TcpPublisherExampleError({
            cause,
            message: "TCP publish command failed.",
          }),
  }).pipe(Effect.andThen(Schema.decodeUnknownEffect(TcpPublishResponse)));
