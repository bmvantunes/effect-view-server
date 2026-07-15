import { describe, expect, it, vi } from "@effect/vitest";
import { Cause, Effect, Exit, Fiber, Queue } from "effect";
import { TestClock } from "effect/testing";
import { TcpPublisherExampleError, type TcpCommand, type TcpPublishResponse } from "./tcp-client";
import { viewServer } from "./view-server.config";

const loadInvalidPublisher = async (response: TcpPublishResponse) => {
  vi.resetModules();
  const programs: Array<Effect.Effect<unknown, TcpPublisherExampleError>> = [];
  const runMain = vi.fn((program: Effect.Effect<unknown, TcpPublisherExampleError>) => {
    programs.push(program);
  });
  const writeCommand = vi.fn(() => Effect.succeed(response));
  vi.doMock("@effect/platform-node", () => ({
    NodeRuntime: { runMain },
  }));
  vi.doMock("./tcp-client", () => ({
    TcpPublisherExampleError,
    writeCommand,
  }));

  await import("./tcp-invalid-publisher");

  return {
    program:
      programs[0] ?? Effect.die(new Error("The invalid publisher did not register a program.")),
    runMain,
    writeCommand,
  };
};

const loadPublisher = async (
  writeCommandImplementation: (command: TcpCommand) => Effect.Effect<TcpPublishResponse, never>,
) => {
  vi.resetModules();
  const programs: Array<Effect.Effect<unknown, TcpPublisherExampleError>> = [];
  const runMain = vi.fn((program: Effect.Effect<unknown, TcpPublisherExampleError>) => {
    programs.push(program);
  });
  const writeCommand = vi.fn(writeCommandImplementation);
  vi.doMock("@effect/platform-node", () => ({
    NodeRuntime: { runMain },
  }));
  vi.doMock("./tcp-client", () => ({
    TcpPublisherExampleError,
    writeCommand,
  }));

  await import("./tcp-publisher");

  return {
    program: programs[0] ?? Effect.die(new Error("The TCP publisher did not register a program.")),
    runMain,
    writeCommand,
  };
};

describe("TCP publisher example entrypoints", () => {
  it("composes the View Server config with TCP ingress runtime options", async () => {
    const runtimeProgram = Effect.void;
    const runMain = vi.fn();
    const runViewServerRuntime = vi.fn(() => runtimeProgram);
    vi.doMock("@effect/platform-node", () => ({
      NodeRuntime: { runMain },
    }));
    vi.doMock("effect-view-server/runtime", () => ({
      runViewServerRuntime,
    }));

    await import("./runtime");

    expect(runViewServerRuntime.mock.calls).toStrictEqual([
      [
        viewServer,
        {
          websocketPort: 8080,
          tcpPublishHost: "127.0.0.1",
          tcpPublishPort: 8081,
        },
      ],
    ]);
    expect(runMain.mock.calls).toStrictEqual([[runtimeProgram]]);
  });

  it.effect("reports the invalid command's typed rejection", () =>
    Effect.gen(function* () {
      const response = {
        ok: false,
        error: {
          _tag: "InvalidRow",
          message: "The row is invalid.",
          phase: "row",
          topic: "orders",
        },
      } satisfies TcpPublishResponse;
      const loaded = yield* Effect.promise(() => loadInvalidPublisher(response));

      yield* loaded.program;

      expect(loaded.writeCommand.mock.calls).toStrictEqual([
        [
          {
            op: "publish",
            topic: "orders",
            row: {
              customerId: "invalid-customer",
              status: "open",
              price: "not-a-number",
              region: "usa",
              updatedAt: 1,
            },
          },
        ],
      ]);
      expect(loaded.runMain.mock.calls).toStrictEqual([[loaded.program]]);
    }),
  );

  it.effect("fails if the invalid command unexpectedly succeeds", () =>
    Effect.gen(function* () {
      const loaded = yield* Effect.promise(() => loadInvalidPublisher({ ok: true }));

      const error = yield* loaded.program.pipe(Effect.flip);

      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("Invalid TCP publish unexpectedly succeeded.");
    }),
  );

  it.effect("publishes an even-timestamped London order", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(2);
      const commands = yield* Queue.unbounded<TcpCommand>();
      const response = { ok: true } satisfies TcpPublishResponse;
      const loaded = yield* Effect.promise(() =>
        loadPublisher((command) => Queue.offer(commands, command).pipe(Effect.as(response))),
      );
      const publisher = yield* loaded.program.pipe(Effect.forkChild({ startImmediately: true }));

      const command = yield* Queue.take(commands);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(publisher);
      const exit = yield* Fiber.await(publisher);

      expect(command).toStrictEqual({
        op: "publish",
        topic: "orders",
        row: {
          id: "tcp-order-2",
          customerId: "tcp-customer-2",
          status: "open",
          price: 10,
          region: "london",
          updatedAt: 2,
        },
      });
      expect(loaded.writeCommand.mock.calls).toStrictEqual([[command]]);
      expect(loaded.runMain.mock.calls).toStrictEqual([[loaded.program]]);
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    }),
  );

  it.effect("publishes an odd-timestamped USA order", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(1);
      const commands = yield* Queue.unbounded<TcpCommand>();
      const response = { ok: true } satisfies TcpPublishResponse;
      const loaded = yield* Effect.promise(() =>
        loadPublisher((command) => Queue.offer(commands, command).pipe(Effect.as(response))),
      );
      const publisher = yield* loaded.program.pipe(Effect.forkChild({ startImmediately: true }));

      const command = yield* Queue.take(commands);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(publisher);
      const exit = yield* Fiber.await(publisher);

      expect(command).toStrictEqual({
        op: "publish",
        topic: "orders",
        row: {
          id: "tcp-order-1",
          customerId: "tcp-customer-1",
          status: "open",
          price: 5,
          region: "usa",
          updatedAt: 1,
        },
      });
      expect(loaded.writeCommand.mock.calls).toStrictEqual([[command]]);
      expect(loaded.runMain.mock.calls).toStrictEqual([[loaded.program]]);
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    }),
  );

  it.effect("fails the publisher program when TCP ingress rejects a row", () =>
    Effect.gen(function* () {
      const response = {
        ok: false,
        error: {
          _tag: "InvalidRow",
          message: "The row is invalid.",
          phase: "row",
          topic: "orders",
        },
      } satisfies TcpPublishResponse;
      const loaded = yield* Effect.promise(() => loadPublisher(() => Effect.succeed(response)));

      const error = yield* loaded.program.pipe(Effect.flip);

      expect(error).toBeInstanceOf(TcpPublisherExampleError);
      expect(error._tag).toBe("TcpPublisherExampleError");
      expect(error.message).toBe("TCP publish failed: The row is invalid.");
    }),
  );
});
