import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Logger,
  Option,
  References,
  Scope,
  Stream,
} from "effect";
import type { StatusEvent } from "@effect-view-server/config";
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

const overflowStatus = (topic: string, queuedEvents: number): StatusEvent => ({
  type: "status",
  topic,
  queryId: "remote",
  status: "closed",
  code: "BackpressureExceeded",
  message: `overflow with ${queuedEvents} queued event(s)`,
});

describe("remote subscription", () => {
  it.effect("streams events and closes without explicit lifecycle hooks", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      const subscription = yield* makeRemoteSubscription({
        clientScope,
        failureStatus,
        overflowStatus,
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
        overflowStatus,
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
        overflowStatus,
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

  it.effect("ends events without a transport error when the client scope closes", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      let failureStatusCount = 0;
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus: (topic, error) => {
          failureStatusCount += 1;
          return failureStatus(topic, error);
        },
        overflowStatus,
        source: Stream.never,
        subscriptionBufferSize: 2,
        topic: "orders",
      });
      const eventsFiber = yield* subscription.events.pipe(Stream.runCollect, Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Scope.close(clientScope, Exit.void);
      const events = yield* Fiber.join(eventsFiber).pipe(Effect.timeout("1 second"));

      expect(failureStatusCount).toBe(0);
      expect(Array.from(events)).toStrictEqual([]);
    }),
  );

  it.effect("does not turn source defects into clean event completion", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        overflowStatus,
        source: Stream.die("source defect"),
        subscriptionBufferSize: 2,
        topic: "orders",
      });

      const defectSeen = yield* subscription.events.pipe(
        Stream.runCollect,
        Effect.matchCause({
          onFailure: Cause.hasDies,
          onSuccess: () => false,
        }),
      );

      expect(defectSeen).toBe(true);
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
        overflowStatus,
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

  it.effect("closes with typed backpressure status when the local event buffer overflows", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      const lifecycleEvents: Array<"open" | "close"> = [];
      let closeCount = 0;
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        lifecycle: {
          onOpen: Effect.sync(() => {
            lifecycleEvents.push("open");
          }),
          onClose: Effect.sync(() => {
            lifecycleEvents.push("close");
            closeCount += 1;
          }),
        },
        overflowStatus,
        source: Stream.make(snapshot, {
          ...snapshot,
          version: 2,
        }),
        subscriptionBufferSize: 1,
        topic: "orders",
      });

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      expect(closeCount).toBe(1);
      expect(lifecycleEvents).toStrictEqual(["open", "close"]);

      const events = yield* subscription.events.pipe(Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "remote",
          status: "closed",
          code: "BackpressureExceeded",
          message: "overflow with 1 queued event(s)",
        },
      ]);
      expect(closeCount).toBe(1);
      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("propagates overflow close defects from lifecycle finalizers", () =>
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
          }).pipe(Effect.andThen(Effect.die("overflow close failed"))),
        },
        overflowStatus,
        source: Stream.make(snapshot, {
          ...snapshot,
          version: 2,
        }),
        subscriptionBufferSize: 1,
        topic: "orders",
      });

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const defectSeen = yield* subscription.events.pipe(
        Stream.runCollect,
        Effect.matchCause({
          onFailure: Cause.hasDies,
          onSuccess: () => false,
        }),
      );

      expect(defectSeen).toBe(true);
      expect(closeCount).toBe(1);
      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("clears stale queued events before waiting for overflow lifecycle close", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      const closeStarted = yield* Deferred.make<void>();
      const closeRelease = yield* Deferred.make<void>();
      const firstEvents = yield* Deferred.make<ReadonlyArray<ViewServerLiveEvent<Row>>>();
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        lifecycle: {
          onOpen: Effect.void,
          onClose: Deferred.succeed(closeStarted, undefined).pipe(
            Effect.andThen(Deferred.await(closeRelease)),
          ),
        },
        overflowStatus,
        source: Stream.make(snapshot, {
          ...snapshot,
          version: 2,
        }),
        subscriptionBufferSize: 1,
        topic: "orders",
      });

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      yield* Deferred.await(closeStarted).pipe(Effect.timeout("1 second"));
      yield* subscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.flatMap((events) => Deferred.succeed(firstEvents, Array.from(events))),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const beforeCloseRelease = yield* Deferred.poll(firstEvents);

      expect(Option.isNone(beforeCloseRelease)).toBe(true);

      yield* Deferred.succeed(closeRelease, undefined);
      const events = yield* Deferred.await(firstEvents);

      expect(events).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "remote",
          status: "closed",
          code: "BackpressureExceeded",
          message: "overflow with 1 queued event(s)",
        },
      ]);
      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect("delivers typed backpressure status when the configured buffer size is zero", () =>
    Effect.gen(function* () {
      const clientScope = yield* Scope.make("parallel");
      const subscription = yield* makeRemoteSubscription<Row, string>({
        clientScope,
        failureStatus,
        overflowStatus,
        source: Stream.make(snapshot, {
          ...snapshot,
          version: 2,
        }),
        subscriptionBufferSize: 0,
        topic: "orders",
      });

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const events = yield* subscription.events.pipe(Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "remote",
          status: "closed",
          code: "BackpressureExceeded",
          message: "overflow with 1 queued event(s)",
        },
      ]);
      yield* Scope.close(clientScope, Exit.void);
    }),
  );

  it.effect(
    "delivers typed backpressure status when the configured buffer size is not finite",
    () =>
      Effect.gen(function* () {
        const clientScope = yield* Scope.make("parallel");
        const subscription = yield* makeRemoteSubscription<Row, string>({
          clientScope,
          failureStatus,
          overflowStatus,
          source: Stream.make(snapshot, {
            ...snapshot,
            version: 2,
          }),
          subscriptionBufferSize: Number.NaN,
          topic: "orders",
        });

        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        const events = yield* subscription.events.pipe(Stream.runCollect);

        expect(Array.from(events)).toStrictEqual([
          {
            type: "status",
            topic: "orders",
            queryId: "remote",
            status: "closed",
            code: "BackpressureExceeded",
            message: "overflow with 1 queued event(s)",
          },
        ]);
        yield* Scope.close(clientScope, Exit.void);
      }),
  );
});
