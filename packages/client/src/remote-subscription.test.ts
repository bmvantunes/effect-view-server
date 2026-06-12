import { describe, expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Logger, References, Scope, Stream } from "effect";
import type { StatusEvent } from "@view-server/config";
import type { ViewServerLiveEvent } from "./live-client";
import { makeRemoteSubscription } from "./remote-subscription";

type Row = {
  readonly id: string;
};

type CapturedLog = {
  readonly cause: Cause.Cause<unknown>;
  readonly logLevel: unknown;
  readonly message: unknown;
};

const makeCapturedLogs = () => {
  const logs: Array<CapturedLog> = [];
  const logger = Logger.make<unknown, void>((options) => {
    logs.push({
      cause: options.cause,
      logLevel: options.logLevel,
      message: options.message,
    });
  });
  return { logger, logs };
};

const snapshot: ViewServerLiveEvent<Row> = {
  type: "snapshot",
  topic: "orders",
  queryId: "query-1",
  version: 1,
  keys: ["order-1"],
  rows: [{ id: "order-1" }],
  totalRows: 1,
};

const failureStatus = (topic: string, error: string): StatusEvent => ({
  type: "status",
  topic,
  queryId: "remote",
  status: "error",
  code: "TransportError",
  message: error,
});

describe("remote subscription", () => {
  it.effect("streams events and closes without explicit lifecycle hooks", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      const subscription = yield* makeRemoteSubscription({
        clientScope,
        failureStatus,
        source: Stream.make(snapshot),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      expect(events[0]).toStrictEqual(snapshot);

      yield* subscription.close();
      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("maps source failures to status events and runs lifecycle finalizers", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      let openCount = 0;
      let closeCount = 0;
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        lifecycle: {
          onOpen: Effect.sync(() => {
            openCount += 1;
          }),
          onClose: Effect.sync(() => {
            closeCount += 1;
          }),
        },
        source: Stream.fail("socket closed"),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      expect(events[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "TransportError",
        message: "socket closed",
      });
      expect(openCount).toBe(1);
      expect(closeCount).toBe(1);

      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("propagates subscription close defects from lifecycle finalizers", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      let closeCount = 0;
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        lifecycle: {
          onOpen: Effect.void,
          onClose: Effect.sync(() => {
            closeCount += 1;
          }).pipe(Effect.andThen(Effect.die("close failed"))),
        },
        source: Stream.make(snapshot),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const closeExit = yield* Effect.exit(subscription.close());

      expect(Exit.isFailure(closeExit)).toBe(true);
      expect(closeCount).toBe(1);
      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("logs and ignores typed lifecycle close failures", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      let closeCount = 0;
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        lifecycle: {
          onOpen: Effect.void,
          onClose: Effect.gen(function* () {
            closeCount += 1;
            return yield* Effect.fail("typed close failure");
          }),
        },
        source: Stream.make(snapshot),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const closeExit = yield* Effect.exit(subscription.close());

      expect(Exit.isSuccess(closeExit)).toBe(true);
      expect(closeCount).toBe(1);
      expect(logs).toHaveLength(1);
      expect(logs[0]?.message).toStrictEqual(["Remote subscription close failed."]);
      expect(logs[0]?.logLevel).toBe("Warn");
      expect(Cause.hasFails(logs[0]?.cause ?? Cause.empty)).toBe(true);
      expect(Cause.hasDies(logs[0]?.cause ?? Cause.empty)).toBe(false);
      expect(Cause.hasInterrupts(logs[0]?.cause ?? Cause.empty)).toBe(false);
      yield* Scope.close(clientScope, Exit.void);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });
});
