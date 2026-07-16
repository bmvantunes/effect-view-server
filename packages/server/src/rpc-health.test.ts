import { describe, expect, it } from "@effect/vitest";
import { makeViewServerClient } from "@effect-view-server/client/remote";
import { type ViewServerHealth, type ViewServerRuntimeError } from "@effect-view-server/config";
import { ViewServerRpcErrorSchema } from "@effect-view-server/protocol";
import { Deferred, Effect, Exit, Fiber, Schema, Scope } from "effect";
import { makeViewServerWebSocketServer } from "./index";
import { makeViewServerRpcHandlers } from "./rpc-handlers";
import {
  createServerTestRuntime,
  kafkaStartFromHealth,
  makeRawRpcClient,
  serverHealthWithOrdersRowCount,
  viewServer,
} from "../test-harness/server";

describe("Real View Server RPC health", () => {
  it.live("serves health from the runtime instead of stale live-client state", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              engine: {
                topics: {
                  ...baseHealth.engine.topics,
                  orders: {
                    ...baseHealth.engine.topics.orders,
                    rowCount: 123,
                  },
                },
              },
            }),
        },
      });
      yield* Effect.addFinalizer(() => server.close);
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      yield* Effect.addFinalizer(() => client.close);

      expect(client.health.value.engine.topics.orders.rowCount).toBe(123);

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("rejects semantically invalid runtime health over unary RPC", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              kafka: {
                startFrom: kafkaStartFromHealth,
                regions: {},
                topics: {
                  source_orders: {
                    status: "ready",
                    sourceTopic: "source_orders",
                    viewServerTopic: "missing",
                    regions: {},
                  },
                },
              },
            }),
        },
      });
      yield* Effect.addFinalizer(() => server.close);
      const raw = yield* makeRawRpcClient(server.url);
      yield* Effect.addFinalizer(() => raw.close);

      const invalidHealth = yield* Effect.flip(raw.rpc["ViewServer.Health"]()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)),
      );
      expect(invalidHealth.code).toBe("InvalidRow");
      expect(invalidHealth.message).toBe("Health payload references unknown topic: missing");

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("closes in-flight remote RPC health reads when the websocket server closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const readStarted = yield* Deferred.make<void>();
      const readInterrupted = yield* Deferred.make<void>();
      let readCount = 0;
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          health: () =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(
              Effect.andThen(Deferred.succeed(readStarted, undefined)),
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(readInterrupted, undefined)),
            ),
        },
      });
      yield* Effect.addFinalizer(() => server.close);
      const raw = yield* makeRawRpcClient(server.url);
      yield* Effect.addFinalizer(() => raw.close);

      const first = yield* raw.rpc["ViewServer.Health"]().pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(first).pipe(Effect.asVoid));
      yield* Deferred.await(readStarted).pipe(Effect.timeout("1 second"));
      const second = yield* raw.rpc["ViewServer.Health"]().pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(second).pipe(Effect.asVoid));
      yield* Effect.yieldNow;
      yield* server.close.pipe(Effect.timeout("1 second"));
      yield* Deferred.await(readInterrupted).pipe(Effect.timeout("1 second"));
      const [firstExit, secondExit] = yield* Effect.all([Fiber.join(first), Fiber.join(second)], {
        concurrency: 2,
      }).pipe(Effect.timeout("1 second"));

      expect(readCount).toBe(1);
      expect(Exit.isFailure(firstExit)).toBe(true);
      expect(Exit.isFailure(secondExit)).toBe(true);
      yield* raw.close;
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("coalesces concurrent RPC health reads while an active read is running", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const firstHealth = serverHealthWithOrdersRowCount(baseHealth, 1);
      const secondHealth = serverHealthWithOrdersRowCount(baseHealth, 2);
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let readCount = 0;
      const healthReads = new Map<
        number,
        Effect.Effect<ViewServerHealth<typeof viewServer.topics>>
      >([
        [
          0,
          Effect.gen(function* () {
            yield* Deferred.succeed(readStarted, undefined);
            yield* Deferred.await(releaseRead);
            return firstHealth;
          }),
        ],
        [1, Effect.succeed(secondHealth)],
      ]);
      const handlerScope = yield* Scope.make("parallel");
      yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Effect.suspend(() => {
                const nextRead =
                  healthReads.get(readCount) ??
                  Effect.die(new Error(`Unexpected health read: ${readCount}`));
                readCount += 1;
                return nextRead;
              }),
          },
        },
        handlerScope,
      );

      const first = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(first).pipe(Effect.asVoid));
      yield* Deferred.await(readStarted).pipe(Effect.timeout("1 second"));
      const second = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(second).pipe(Effect.asVoid));
      yield* Deferred.succeed(releaseRead, undefined);

      const [firstResult, secondResult] = yield* Effect.all(
        [Fiber.join(first), Fiber.join(second)],
        { concurrency: 2 },
      ).pipe(Effect.timeout("1 second"));
      const thirdResult = yield* handlers["ViewServer.Health"]();

      expect(readCount).toBe(2);
      expect(firstResult).toStrictEqual(firstHealth);
      expect(secondResult).toStrictEqual(firstHealth);
      expect(thirdResult).toStrictEqual(secondHealth);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("clears failed RPC health reads so later callers retry", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const recoveredHealth = serverHealthWithOrdersRowCount(baseHealth, 3);
      const healthError: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "health unavailable",
      };
      let readCount = 0;
      const healthReads = new Map<
        number,
        Effect.Effect<ViewServerHealth<typeof viewServer.topics>, ViewServerRuntimeError>
      >([
        [0, Effect.fail(healthError)],
        [1, Effect.succeed(recoveredHealth)],
      ]);
      const handlerScope = yield* Scope.make("parallel");
      yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Effect.suspend(() => {
                const nextRead =
                  healthReads.get(readCount) ??
                  Effect.die(new Error(`Unexpected health read: ${readCount}`));
                readCount += 1;
                return nextRead;
              }),
          },
        },
        handlerScope,
      );

      const failedHealth = yield* Effect.flip(handlers["ViewServer.Health"]());
      const retriedHealth = yield* handlers["ViewServer.Health"]();

      expect(readCount).toBe(2);
      expect(failedHealth).toStrictEqual(healthError);
      expect(retriedHealth).toStrictEqual(recoveredHealth);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("keeps shared RPC health reads alive when the leader caller is interrupted", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const baseHealth = yield* inMemory.client.health();
      const firstHealth = serverHealthWithOrdersRowCount(baseHealth, 3);
      const recoveredHealth = serverHealthWithOrdersRowCount(baseHealth, 4);
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let readCount = 0;
      const healthReads = new Map<
        number,
        Effect.Effect<ViewServerHealth<typeof viewServer.topics>, ViewServerRuntimeError>
      >([
        [
          0,
          Effect.gen(function* () {
            readCount += 1;
            yield* Deferred.succeed(readStarted, undefined);
            yield* Deferred.await(releaseRead);
            return firstHealth;
          }),
        ],
        [
          1,
          Effect.sync(() => {
            readCount += 1;
            return recoveredHealth;
          }),
        ],
      ]);
      const handlerScope = yield* Scope.make("parallel");
      yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Effect.suspend(
                () =>
                  healthReads.get(readCount) ??
                  Effect.die(new Error(`Unexpected health read: ${readCount}`)),
              ),
          },
        },
        handlerScope,
      );

      const leader = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(leader).pipe(Effect.asVoid));
      yield* Deferred.await(readStarted).pipe(Effect.timeout("1 second"));
      const follower = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(follower).pipe(Effect.asVoid));
      yield* Fiber.interrupt(leader).pipe(
        Effect.timeout("1 second"),
        Effect.onError(() => Deferred.succeed(releaseRead, undefined).pipe(Effect.asVoid)),
      );
      const leaderExit = yield* Fiber.await(leader).pipe(Effect.timeout("1 second"));
      yield* Deferred.succeed(releaseRead, undefined);
      const followerHealth = yield* Fiber.join(follower).pipe(Effect.timeout("1 second"));
      const retriedHealth = yield* handlers["ViewServer.Health"]();

      expect(followerHealth).toStrictEqual(firstHealth);
      expect(Exit.hasInterrupts(leaderExit)).toBe(true);
      expect(readCount).toBe(2);
      expect(retriedHealth).toStrictEqual(recoveredHealth);
      yield* Scope.close(handlerScope, Exit.void);
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live("interrupts active RPC health reads when the handler scope closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const readStarted = yield* Deferred.make<void>();
      const readInterrupted = yield* Deferred.make<void>();
      const handlerScope = yield* Scope.make("parallel");
      yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: {
            health: () =>
              Deferred.succeed(readStarted, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(readInterrupted, undefined)),
              ),
          },
        },
        handlerScope,
      );

      const healthFiber = yield* handlers["ViewServer.Health"]().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(healthFiber).pipe(Effect.asVoid));
      yield* Deferred.await(readStarted).pipe(Effect.timeout("1 second"));
      yield* Scope.close(handlerScope, Exit.void).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(readInterrupted).pipe(Effect.timeout("1 second"));
      const healthExit = yield* Fiber.await(healthFiber).pipe(Effect.timeout("1 second"));

      expect(Exit.hasInterrupts(healthExit)).toBe(true);
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );

  it.live(
    "does not wait forever when closing handler scope with a non-cancellable active RPC health worker",
    () =>
      Effect.gen(function* () {
        const inMemory = createServerTestRuntime(viewServer);
        yield* Effect.addFinalizer(() => inMemory.close);
        const baseHealth = yield* inMemory.client.health();
        const readStarted = yield* Deferred.make<void>();
        const releaseRead = yield* Deferred.make<void>();
        const workerReadFinished = yield* Deferred.make<void>();
        const handlerScope = yield* Scope.make("parallel");
        yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
        const handlers = makeViewServerRpcHandlers(
          viewServer,
          {
            liveClient: inMemory.liveClient,
            runtime: {
              health: () =>
                Effect.uninterruptible(
                  Deferred.succeed(readStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseRead)),
                    Effect.as(baseHealth),
                    Effect.ensuring(Deferred.succeed(workerReadFinished, undefined)),
                  ),
                ),
            },
          },
          handlerScope,
        );

        const healthFiber = yield* handlers["ViewServer.Health"]().pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.addFinalizer(() => Fiber.interrupt(healthFiber).pipe(Effect.asVoid));
        yield* Effect.addFinalizer(() =>
          Deferred.succeed(releaseRead, undefined).pipe(Effect.asVoid),
        );
        yield* Deferred.await(readStarted).pipe(Effect.timeout("1 second"));
        yield* Scope.close(handlerScope, Exit.void).pipe(Effect.timeout("1 second"));
        const healthExit = yield* Fiber.await(healthFiber).pipe(Effect.timeout("1 second"));
        yield* Deferred.succeed(releaseRead, undefined);
        yield* Deferred.await(workerReadFinished).pipe(Effect.timeout("1 second"));
        yield* Effect.yieldNow;

        expect(Exit.hasInterrupts(healthExit)).toBe(true);
        yield* inMemory.close;
      }).pipe(Effect.scoped),
  );

  it.live("interrupts RPC health reads started after the handler scope closes", () =>
    Effect.gen(function* () {
      const inMemory = createServerTestRuntime(viewServer);
      yield* Effect.addFinalizer(() => inMemory.close);
      const handlerScope = yield* Scope.make("parallel");
      yield* Effect.addFinalizer(() => Scope.close(handlerScope, Exit.void));
      const handlers = makeViewServerRpcHandlers(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: inMemory.client,
        },
        handlerScope,
      );

      yield* Scope.close(handlerScope, Exit.void);
      const healthExit = yield* handlers["ViewServer.Health"]().pipe(Effect.exit);

      expect(Exit.hasInterrupts(healthExit)).toBe(true);
      yield* inMemory.close;
    }).pipe(Effect.scoped),
  );
});
