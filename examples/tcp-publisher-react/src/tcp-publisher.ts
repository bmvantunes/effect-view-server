import { NodeRuntime } from "@effect/platform-node";
import { Clock, Effect, Schedule } from "effect";
import * as Net from "node:net";

type TcpCommand = {
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

const writeCommand = (command: TcpCommand) =>
  Effect.tryPromise({
    try: () =>
      new Promise<void>((resolve, reject) => {
        let isSettled = false;
        const socket = Net.createConnection({ host: "127.0.0.1", port: 8081 }, () => {
          socket.write(`${JSON.stringify(command)}\n`);
        });
        const finish = () => {
          if (!isSettled) {
            isSettled = true;
            socket.end();
            resolve();
          }
        };
        const fail = (cause: Error) => {
          if (!isSettled) {
            isSettled = true;
            socket.destroy();
            reject(cause);
          }
        };
        socket.setTimeout(5_000, () =>
          fail(new Error("Timed out waiting for TCP publish acknowledgement.")),
        );
        socket.once("error", fail);
        socket.once("data", finish);
      }),
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  });

const publishNext = (index: number) =>
  writeCommand({
    op: "publish",
    topic: "orders",
    row: {
      id: `tcp-order-${index}`,
      customerId: `tcp-customer-${index}`,
      status: "open",
      price: index * 5,
      region: index % 2 === 0 ? "london" : "usa",
      updatedAt: index,
    },
  });

NodeRuntime.runMain(
  Effect.repeat(
    Effect.gen(function* () {
      const next = yield* Clock.currentTimeMillis;
      yield* publishNext(next);
      yield* Effect.logInfo(`Published tcp-order-${next}`);
    }),
    Schedule.spaced("1 second"),
  ),
);
