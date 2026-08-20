import { describe, expect, it } from "@effect/vitest";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { Effect, Fiber, Schedule, Stream } from "effect";
import { makeViewServerRuntime } from "./index";
import {
  closeTestTcpServer,
  reserveTcpPort,
  waitForTransportHealth,
} from "../test-harness/runtime";
import { order, viewServer } from "../test-harness/runtime-config";

const acquireRuntime = (port: number) =>
  Effect.acquireRelease(
    Effect.retry(
      makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: port,
      }),
      Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
    ),
    (runtime) => runtime.close,
  );

const assertOneRecoveredStream = Effect.fn("WebSocketReconnect.test.assertOneRecoveredStream")(
  function* (runtime: Effect.Success<ReturnType<typeof makeViewServerRuntime>>) {
    yield* waitForTransportHealth(runtime.client.health, {
      activeClients: 1,
      activeStreams: 1,
    });
    yield* Effect.sleep("750 millis");
    const health = yield* runtime.client.health();
    expect({
      activeClients: health.transport.activeClients,
      activeStreams: health.transport.activeStreams,
    }).toStrictEqual({ activeClients: 1, activeStreams: 1 });
  },
);

describe("WebSocket subscription recovery", () => {
  it.live("delivers one fresh snapshot after every server restart", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const reservation = yield* reserveTcpPort();
        yield* closeTestTcpServer(reservation.server);
        const port = reservation.port;

        const first = yield* acquireRuntime(port);
        yield* first.client.publish("orders", order("a", 10));
        const client = yield* Effect.acquireRelease(
          makeViewServerClient(viewServer, { url: first.url }),
          (remoteClient) => remoteClient.close,
        );
        const subscription = yield* Effect.acquireRelease(
          client.subscribe("orders", {
            select: ["id", "price"],
            limit: 10,
          }),
          (activeSubscription) => activeSubscription.close().pipe(Effect.ignore),
        );
        const snapshots: Array<
          Extract<Stream.Success<typeof subscription.events>, { readonly type: "snapshot" }>
        > = [];
        const eventsFiber = yield* subscription.events.pipe(
          Stream.filter((event) => event.type === "snapshot"),
          Stream.runForEach((event) =>
            Effect.sync(() => {
              snapshots.push(event);
            }),
          ),
          Effect.forkChild,
        );
        yield* assertOneRecoveredStream(first);

        yield* first.close;
        const second = yield* acquireRuntime(port);
        yield* assertOneRecoveredStream(second);

        const controlClient = yield* Effect.acquireRelease(
          makeViewServerClient(viewServer, { url: second.url }),
          (remoteClient) => remoteClient.close,
        );
        const controlSubscription = yield* Effect.acquireRelease(
          controlClient.subscribe("orders", {
            select: ["id", "price"],
            limit: 10,
          }),
          (activeSubscription) => activeSubscription.close().pipe(Effect.ignore),
        );
        const controlSnapshot = yield* controlSubscription.events.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.timeout("1 second"),
        );
        expect(controlSnapshot).toStrictEqual([
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-1",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          },
        ]);
        yield* waitForTransportHealth(second.client.health, {
          activeClients: 2,
          activeStreams: 2,
        });
        yield* controlSubscription.close();
        yield* controlClient.close;
        yield* waitForTransportHealth(second.client.health, {
          activeClients: 1,
          activeStreams: 1,
        });

        yield* second.close;
        const third = yield* acquireRuntime(port);
        yield* assertOneRecoveredStream(third);
        yield* Effect.sleep("5 millis").pipe(
          Effect.repeat({
            schedule: Schedule.recurs(50),
            until: () => snapshots.length === 3,
          }),
        );

        expect(snapshots).toStrictEqual([
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", price: 10 }],
            totalRows: 1,
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 0,
            keys: [],
            rows: [],
            totalRows: 0,
          },
        ]);

        yield* subscription.close();
        yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));
        yield* client.close;
        yield* waitForTransportHealth(third.client.health, {
          activeClients: 0,
          activeStreams: 0,
        });
      }),
    ),
  );

  it.live("does not resubscribe after the client closes during a pending retry", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const reservation = yield* reserveTcpPort();
        yield* closeTestTcpServer(reservation.server);
        const port = reservation.port;

        const first = yield* acquireRuntime(port);
        const client = yield* Effect.acquireRelease(
          makeViewServerClient(viewServer, { url: first.url }),
          (remoteClient) => remoteClient.close,
        );
        const subscription = yield* Effect.acquireRelease(
          client.subscribe("orders", { select: ["id"], limit: 10 }),
          (activeSubscription) => activeSubscription.close().pipe(Effect.ignore),
        );
        yield* assertOneRecoveredStream(first);

        yield* first.close;
        yield* Effect.sleep("100 millis");
        yield* client.close;
        const second = yield* acquireRuntime(port);
        yield* Effect.sleep("750 millis");
        const health = yield* second.client.health();

        expect({
          activeClients: health.transport.activeClients,
          activeStreams: health.transport.activeStreams,
        }).toStrictEqual({ activeClients: 0, activeStreams: 0 });
        yield* subscription.close();
      }),
    ),
  );
});
