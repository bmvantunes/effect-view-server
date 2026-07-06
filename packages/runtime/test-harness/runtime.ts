import { type TransportHealth } from "@effect-view-server/config";
import { Effect, Schedule, Schema } from "effect";
import * as Net from "node:net";

const HealthJson = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  engine: Schema.Struct({
    topics: Schema.Struct({
      orders: Schema.Struct({
        rowCount: Schema.Number,
      }),
    }),
  }),
});

class RuntimeHealthJsonParseError extends Schema.TaggedErrorClass<RuntimeHealthJsonParseError>()(
  "RuntimeHealthJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

class RuntimeJsonParseError extends Schema.TaggedErrorClass<RuntimeJsonParseError>()(
  "RuntimeJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

export class RuntimeTestFailure extends Schema.TaggedErrorClass<RuntimeTestFailure>()(
  "RuntimeTestFailure",
  {
    message: Schema.String,
  },
) {}

export class RuntimeTcpTestFailure extends Schema.TaggedErrorClass<RuntimeTcpTestFailure>()(
  "RuntimeTcpTestFailure",
  {
    cause: Schema.Unknown,
    message: Schema.String,
  },
) {}

const TcpPublishResponse = Schema.Union([
  Schema.Struct({
    ok: Schema.Literal(true),
  }),
  Schema.Struct({
    ok: Schema.Literal(false),
    error: Schema.Struct({
      _tag: Schema.String,
      code: Schema.optional(Schema.String),
      message: Schema.String,
      phase: Schema.optional(Schema.String),
      status: Schema.optional(Schema.Number),
      topic: Schema.optional(Schema.String),
    }),
  }),
]);

const TestTcpAddress = Schema.Struct({
  address: Schema.String,
  family: Schema.String,
  port: Schema.Number,
});

export const nullRecord = <Value>(
  entries: ReadonlyArray<readonly [string, Value]>,
): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  for (const [key, value] of entries) {
    record[key] = value;
  }
  return record;
};

export const fetchHealth = Effect.fn("ViewServerRuntime.test.health.fetch")(function* (
  url: string,
) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new RuntimeHealthJsonParseError({ cause }),
  });
  const health = yield* Schema.decodeUnknownEffect(HealthJson)(value);
  return { response, health };
});

export const fetchText = Effect.fn("ViewServerRuntime.test.text.fetch")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  return { response, text };
});

export const fetchJson = Effect.fn("ViewServerRuntime.test.json.fetch")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new RuntimeJsonParseError({ cause }),
  });
  return { response, value };
});

const tcpUrlConnectHost = (hostname: string): string =>
  hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;

export const connectTcpPublishSocket = Effect.fn("ViewServerRuntime.test.tcp.connect")(function* (
  url: string,
) {
  const parsedUrl = new URL(url);
  const port = Number(parsedUrl.port);
  if (!Number.isSafeInteger(port)) {
    return yield* new RuntimeTcpTestFailure({
      message: "TCP publish URL did not include a valid port.",
      cause: url,
    });
  }
  return yield* Effect.callback<Net.Socket, RuntimeTcpTestFailure>((resume) => {
    const socket = Net.createConnection({
      host: tcpUrlConnectHost(parsedUrl.hostname),
      port,
    });
    socket.setEncoding("utf8");
    socket.once("connect", () => {
      resume(Effect.succeed(socket));
    });
    socket.once("error", (cause) => {
      resume(
        Effect.fail(
          new RuntimeTcpTestFailure({
            message: "TCP publish socket failed to connect.",
            cause,
          }),
        ),
      );
    });
  });
});

export const readTcpPublishResponse = Effect.fn("ViewServerRuntime.test.tcp.response.read")(
  function* (socket: Net.Socket) {
    const line = yield* Effect.callback<string, RuntimeTcpTestFailure>((resume) => {
      let buffer = "";
      socket.on("data", (chunk: string) => {
        buffer += chunk;
        const newlineIndex = buffer.indexOf("\n");
        if (newlineIndex >= 0) {
          resume(Effect.succeed(buffer.slice(0, newlineIndex)));
        }
      });
      socket.once("error", (cause) => {
        resume(
          Effect.fail(
            new RuntimeTcpTestFailure({
              message: "TCP publish socket failed while reading response.",
              cause,
            }),
          ),
        );
      });
    });
    const value = yield* Effect.try({
      try: (): unknown => JSON.parse(line),
      catch: (cause) =>
        new RuntimeTcpTestFailure({
          message: "TCP publish response was not valid JSON.",
          cause,
        }),
    });
    return yield* Schema.decodeUnknownEffect(TcpPublishResponse)(value).pipe(
      Effect.mapError(
        (cause) =>
          new RuntimeTcpTestFailure({
            message: "TCP publish response did not match the test schema.",
            cause,
          }),
      ),
    );
  },
);

export const readTcpPublishResponses = Effect.fn("ViewServerRuntime.test.tcp.responses.read")(
  function* (socket: Net.Socket, count: number) {
    const lines = yield* Effect.callback<ReadonlyArray<string>, RuntimeTcpTestFailure>((resume) => {
      let buffer = "";
      const responses: Array<string> = [];
      socket.on("data", (chunk: string) => {
        buffer += chunk;
        let newlineIndex = buffer.indexOf("\n");
        while (newlineIndex >= 0) {
          responses.push(buffer.slice(0, newlineIndex));
          buffer = buffer.slice(newlineIndex + 1);
          newlineIndex = buffer.indexOf("\n");
        }
        if (responses.length === count) {
          resume(Effect.succeed(responses));
        }
      });
      socket.once("error", (cause) => {
        resume(
          Effect.fail(
            new RuntimeTcpTestFailure({
              message: "TCP publish socket failed while reading responses.",
              cause,
            }),
          ),
        );
      });
    });
    return yield* Effect.forEach(lines, (line) =>
      Effect.try({
        try: (): unknown => JSON.parse(line),
        catch: (cause) =>
          new RuntimeTcpTestFailure({
            message: "TCP publish response was not valid JSON.",
            cause,
          }),
      }).pipe(
        Effect.flatMap((value) => Schema.decodeUnknownEffect(TcpPublishResponse)(value)),
        Effect.mapError(
          (cause) =>
            new RuntimeTcpTestFailure({
              message: "TCP publish response did not match the test schema.",
              cause,
            }),
        ),
      ),
    );
  },
);

export const sendTcpPublishLine = Effect.fn("ViewServerRuntime.test.tcp.line.send")(function* (
  url: string,
  line: string,
) {
  const socket = yield* Effect.acquireRelease(connectTcpPublishSocket(url), (socket) =>
    Effect.sync(() => socket.destroy()),
  );
  socket.write(`${line}\n`);
  return yield* readTcpPublishResponse(socket).pipe(Effect.timeout("1 second"));
});

export const sendTcpPublishCommand = Effect.fn("ViewServerRuntime.test.tcp.command.send")(
  function* (url: string, command: object) {
    return yield* sendTcpPublishLine(url, JSON.stringify(command));
  },
);

export const closeTestTcpServer = (server: Net.Server): Effect.Effect<void> =>
  Effect.promise(
    () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  );

export const reserveTcpPort = Effect.fn("ViewServerRuntime.test.tcp.port.reserve")(function* () {
  const server = yield* Effect.callback<Net.Server, RuntimeTcpTestFailure>((resume) => {
    const nextServer = Net.createServer();
    nextServer.once("error", (cause) => {
      resume(
        Effect.fail(
          new RuntimeTcpTestFailure({
            message: "Test TCP server failed to reserve a port.",
            cause,
          }),
        ),
      );
    });
    nextServer.listen({ host: "127.0.0.1", port: 0 }, () => {
      resume(Effect.succeed(nextServer));
    });
  });
  const address = yield* Schema.decodeUnknownEffect(TestTcpAddress)(server.address()).pipe(
    Effect.mapError(
      (cause) =>
        new RuntimeTcpTestFailure({
          message: "Test TCP server produced an invalid listen address.",
          cause,
        }),
    ),
  );
  return { server, port: address.port };
});

export const waitForTransportHealth = Effect.fn("ViewServerRuntime.test.transportHealth.wait")(
  function* (
    health: () => Effect.Effect<{ readonly transport: TransportHealth }, unknown>,
    expected: {
      readonly activeClients: number;
      readonly activeStreams: number;
    },
  ) {
    return yield* health().pipe(
      Effect.map((value) => value.transport),
      Effect.repeat({
        schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
        until: (transport) =>
          transport.activeClients === expected.activeClients &&
          transport.activeStreams === expected.activeStreams,
      }),
    );
  },
);
