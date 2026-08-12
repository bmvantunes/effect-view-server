import { describe, expect, it } from "@effect/vitest";
import type {
  RuntimeDependency,
  RuntimeHeartbeat,
  RuntimeSourceReportingSnapshot,
} from "@effect-view-server/runtime-core";
import {
  Cause,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Logger,
  Option,
  Queue,
  Scope,
  Stream,
  SubscriptionRef,
  References,
} from "effect";
import { TestClock } from "effect/testing";
import { makeRuntimeReporting, startingHeartbeat } from "./runtime-reporting";

const healthy: RuntimeSourceReportingSnapshot = {
  heartbeat: { status: "Ready", problems: [] },
  dependencies: [
    {
      dependency: "kafka",
      target: "tokyo",
      endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
      status: "Ready",
      issues: [],
    },
  ],
};

const unhealthy: RuntimeSourceReportingSnapshot = {
  heartbeat: { status: "WaitingToRetry", problems: ["dependency"] },
  dependencies: [
    {
      dependency: "kafka",
      target: "tokyo",
      endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
      status: "WaitingToRetry",
      issues: [],
    },
  ],
};

const detailed = (schemaId: string): RuntimeSourceReportingSnapshot => ({
  heartbeat: { status: "WaitingToRetry", problems: ["dependency"] },
  dependencies: [
    {
      dependency: "schema-registry",
      target: "tokyo",
      endpoints: ["https://registry.kafka-tky.com"],
      status: "WaitingToRetry",
      issues: [
        {
          source: "orders",
          code: "KafkaSchemaRegistrySchemaMismatch",
          message: `Schema ID ${schemaId} is incompatible.`,
          attributes: [
            { name: "subject", value: "source-orders-value" },
            { name: "schemaId", value: schemaId },
          ],
        },
      ],
    },
  ],
});

describe("Runtime reporting", () => {
  it.effect("preserves callback interruption", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make("sequential");
      const callback = yield* startingHeartbeat(scope, {
        heartbeatInterval: Duration.seconds(1),
        dependenciesInterval: Duration.seconds(1),
        changeInterval: Duration.millis(300),
        onHeartbeat: () => Effect.interrupt,
        onDependenciesUpdate: () => Effect.void,
      });
      const exit = yield* Fiber.await(callback);

      expect(
        Exit.match(exit, {
          onFailure: Cause.hasInterruptsOnly,
          onSuccess: () => false,
        }),
      ).toBe(true);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("preserves interruption from a mixed callback cause", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make("sequential");
      const callback = yield* startingHeartbeat(scope, {
        heartbeatInterval: Duration.seconds(1),
        dependenciesInterval: Duration.seconds(1),
        changeInterval: Duration.millis(300),
        onHeartbeat: () =>
          Effect.failCause(
            Cause.fromReasons([
              Cause.makeDieReason("callback defect"),
              Cause.makeInterruptReason(),
            ]),
          ),
        onDependenciesUpdate: () => Effect.void,
      });
      const exit = yield* Fiber.await(callback);

      expect(
        Exit.match(exit, {
          onFailure: Cause.hasInterruptsOnly,
          onSuccess: () => false,
        }),
      ).toBe(true);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("logs a synchronous lifecycle callback construction defect", () => {
    const logs: Array<unknown> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push(options.message);
    });
    return Effect.gen(function* () {
      const scope = yield* Scope.make("sequential");
      const callback = yield* startingHeartbeat(scope, {
        heartbeatInterval: Duration.seconds(1),
        dependenciesInterval: Duration.seconds(1),
        changeInterval: Duration.millis(300),
        onHeartbeat: () => {
          throw new Error("synchronous lifecycle callback defect");
        },
        onDependenciesUpdate: () => Effect.void,
      });

      expect(Exit.isSuccess(yield* Fiber.await(callback))).toBe(true);
      expect(logs.map((message) => (Array.isArray(message) ? message[0] : message))).toStrictEqual([
        "View Server heartbeat reporting callback failed.",
      ]);
      yield* Scope.close(scope, Exit.void);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("does not let the change debounce replace a shorter periodic cadence", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const source = yield* SubscriptionRef.make(healthy);
      const scope = yield* Scope.make("sequential");
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.millis(100),
          dependenciesInterval: Duration.hours(1),
          changeInterval: Duration.millis(300),
          onHeartbeat: (heartbeat) => Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid),
          onDependenciesUpdate: () => Effect.void,
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: SubscriptionRef.changes(source),
        },
      );
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);

      for (let tick = 0; tick < 3; tick += 1) {
        yield* TestClock.adjust("100 millis");
        yield* Effect.yieldNow;
        expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);
      }
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("observes a semantic change replayed when the listener starts", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const dependencies = yield* Queue.unbounded<ReadonlyArray<RuntimeDependency>>();
      const source = yield* SubscriptionRef.make(healthy);
      const scope = yield* Scope.make("sequential");
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          changeInterval: Duration.millis(300),
          onHeartbeat: (heartbeat) => Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid),
          onDependenciesUpdate: (snapshot) =>
            Queue.offer(dependencies, snapshot).pipe(Effect.asVoid),
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: Stream.unwrap(
            SubscriptionRef.set(source, unhealthy).pipe(Effect.as(SubscriptionRef.changes(source))),
          ),
        },
      );
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(unhealthy.heartbeat);
      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(dependencies)).toStrictEqual(unhealthy.dependencies);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("emits lifecycle, independent cadence, and coalesced semantic changes", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const dependencies = yield* Queue.unbounded<ReadonlyArray<RuntimeDependency>>();
      const options = {
        heartbeatInterval: Duration.seconds(10),
        dependenciesInterval: Duration.seconds(20),
        changeInterval: Duration.millis(300),
        onHeartbeat: (heartbeat: RuntimeHeartbeat) =>
          Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid),
        onDependenciesUpdate: (snapshot: ReadonlyArray<RuntimeDependency>) =>
          Queue.offer(dependencies, snapshot).pipe(Effect.asVoid),
      } as const;

      const scope = yield* Scope.make("sequential");
      yield* startingHeartbeat(scope, options);
      expect(yield* Queue.take(heartbeats)).toStrictEqual({ status: "Starting", problems: [] });

      const source = yield* SubscriptionRef.make(healthy);
      const reporter = yield* makeRuntimeReporting(scope, options, {
        snapshot: SubscriptionRef.get(source),
        changes: SubscriptionRef.changes(source),
      });
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual({ status: "Ready", problems: [] });
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);

      yield* TestClock.adjust("10 seconds");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual({ status: "Ready", problems: [] });
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);

      yield* TestClock.adjust("10 seconds");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual({ status: "Ready", problems: [] });
      expect(yield* Queue.take(dependencies)).toStrictEqual(healthy.dependencies);

      yield* SubscriptionRef.set(source, unhealthy);
      yield* Effect.yieldNow;
      yield* SubscriptionRef.set(source, healthy);
      yield* SubscriptionRef.set(source, unhealthy);
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);

      yield* TestClock.adjust("299 millis");
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);
      yield* TestClock.adjust("1 millis");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(unhealthy.heartbeat);
      expect(yield* Queue.take(dependencies)).toStrictEqual(unhealthy.dependencies);
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);

      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);

      yield* SubscriptionRef.set(source, healthy);
      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);
      expect(yield* Queue.take(dependencies)).toStrictEqual(healthy.dependencies);
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);

      yield* reporter.stopping;
      expect(yield* Queue.take(heartbeats)).toStrictEqual({ status: "Stopping", problems: [] });
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("coalesces rebuilt dependency issues and emits semantic issue changes", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const dependencies = yield* Queue.unbounded<ReadonlyArray<RuntimeDependency>>();
      const source = yield* SubscriptionRef.make(detailed("42"));
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          changeInterval: Duration.millis(300),
          onHeartbeat: (heartbeat) => Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid),
          onDependenciesUpdate: (snapshot) =>
            Queue.offer(dependencies, snapshot).pipe(Effect.asVoid),
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: SubscriptionRef.changes(source),
        },
      );
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(detailed("42").heartbeat);

      yield* SubscriptionRef.set(source, detailed("42"));
      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);

      yield* SubscriptionRef.set(source, detailed("43"));
      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(detailed("43").heartbeat);
      expect(yield* Queue.take(dependencies)).toStrictEqual(detailed("43").dependencies);
    }).pipe(Effect.scoped),
  );

  it.effect("logs callback defects and keeps later emissions alive", () => {
    const logs: Array<{ readonly logLevel: unknown; readonly message: unknown }> = [];
    const logger = Logger.make<unknown, void>((options) => {
      logs.push({ logLevel: options.logLevel, message: options.message });
    });
    return Effect.gen(function* () {
      let heartbeatAttempts = 0;
      let dependencyAttempts = 0;
      const deliveredHeartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const deliveredDependencies = yield* Queue.unbounded<ReadonlyArray<RuntimeDependency>>();
      const source = yield* SubscriptionRef.make(healthy);
      const scope = yield* Scope.make("sequential");
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.seconds(1),
          dependenciesInterval: Duration.seconds(2),
          changeInterval: Duration.millis(1),
          onHeartbeat: (heartbeat) => {
            heartbeatAttempts += 1;
            if (heartbeatAttempts === 1) {
              throw new Error("synchronous heartbeat callback defect");
            }
            return Queue.offer(deliveredHeartbeats, heartbeat).pipe(Effect.asVoid);
          },
          onDependenciesUpdate: (dependencies) => {
            dependencyAttempts += 1;
            if (dependencyAttempts === 1) {
              throw new Error("synchronous dependency callback defect");
            }
            return Queue.offer(deliveredDependencies, dependencies).pipe(Effect.asVoid);
          },
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: Stream.drop(SubscriptionRef.changes(source), 0),
        },
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(deliveredHeartbeats)).toStrictEqual(healthy.heartbeat);
      yield* TestClock.adjust("3 seconds");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(deliveredDependencies)).toStrictEqual(healthy.dependencies);
      expect(heartbeatAttempts).toBe(5);
      expect(dependencyAttempts).toBe(2);
      expect(
        logs.map(({ logLevel, message }) => ({
          logLevel,
          message: Array.isArray(message) ? message[0] : message,
        })),
      ).toStrictEqual([
        {
          logLevel: "Warn",
          message: "View Server heartbeat reporting callback failed.",
        },
        {
          logLevel: "Warn",
          message: "View Server dependencies reporting callback failed.",
        },
      ]);
      yield* Scope.close(scope, Exit.void);
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("emits both callbacks when only dependency target state changes", () =>
    Effect.gen(function* () {
      const tokyoUnhealthy: RuntimeSourceReportingSnapshot = {
        heartbeat: unhealthy.heartbeat,
        dependencies: [
          {
            dependency: "kafka",
            target: "oregon",
            endpoints: ["b-1.kafka-oregon.com"],
            status: "Ready",
            issues: [],
          },
          {
            dependency: "kafka",
            target: "tokyo",
            endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
            status: "WaitingToRetry",
            issues: [],
          },
        ],
      };
      const oregonUnhealthy: RuntimeSourceReportingSnapshot = {
        heartbeat: unhealthy.heartbeat,
        dependencies: [
          {
            dependency: "kafka",
            target: "oregon",
            endpoints: ["b-1.kafka-oregon.com"],
            status: "WaitingToRetry",
            issues: [],
          },
          {
            dependency: "kafka",
            target: "tokyo",
            endpoints: ["b-1.kafka-tky.com", "b-2.kafka-tky.com"],
            status: "Ready",
            issues: [],
          },
        ],
      };
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const dependencies = yield* Queue.unbounded<ReadonlyArray<RuntimeDependency>>();
      const source = yield* SubscriptionRef.make(tokyoUnhealthy);
      const scope = yield* Scope.make("sequential");
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          changeInterval: Duration.millis(300),
          onHeartbeat: (heartbeat) => Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid),
          onDependenciesUpdate: (snapshot) =>
            Queue.offer(dependencies, snapshot).pipe(Effect.asVoid),
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: SubscriptionRef.changes(source),
        },
      );
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(unhealthy.heartbeat);

      yield* SubscriptionRef.set(source, oregonUnhealthy);
      yield* Effect.yieldNow;
      expect(yield* Queue.take(dependencies)).toStrictEqual(oregonUnhealthy.dependencies);
      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(unhealthy.heartbeat);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("does not publish dependencies when only self provenance changes", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const dependencies = yield* Queue.unbounded<ReadonlyArray<RuntimeDependency>>();
      const source = yield* SubscriptionRef.make(healthy);
      const scope = yield* Scope.make("sequential");
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          changeInterval: Duration.millis(300),
          onHeartbeat: (heartbeat) => Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid),
          onDependenciesUpdate: (snapshot) =>
            Queue.offer(dependencies, snapshot).pipe(Effect.asVoid),
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: SubscriptionRef.changes(source),
        },
      );
      yield* Effect.yieldNow;
      yield* Queue.take(heartbeats);

      yield* SubscriptionRef.set(source, {
        heartbeat: { status: "WaitingToRetry", problems: ["self"] },
        dependencies: healthy.dependencies,
      });
      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;

      expect(yield* Queue.take(heartbeats)).toStrictEqual({
        status: "WaitingToRetry",
        problems: ["self"],
      });
      expect(Option.isNone(yield* Queue.poll(dependencies))).toBe(true);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("does not acknowledge a semantic change that arrives during its callback", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const callbackStarted = yield* Deferred.make<void>();
      const releaseCallback = yield* Deferred.make<void>();
      const source = yield* SubscriptionRef.make(healthy);
      const scope = yield* Scope.make("sequential");
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.hours(1),
          dependenciesInterval: Duration.hours(1),
          changeInterval: Duration.millis(300),
          onHeartbeat: (heartbeat) =>
            heartbeat.status === "WaitingToRetry"
              ? Deferred.succeed(callbackStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseCallback)),
                  Effect.andThen(Queue.offer(heartbeats, heartbeat)),
                  Effect.asVoid,
                )
              : Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid),
          onDependenciesUpdate: () => Effect.void,
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: SubscriptionRef.changes(source),
        },
      );
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);

      yield* SubscriptionRef.set(source, unhealthy);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("300 millis");
      yield* Deferred.await(callbackStarted);
      yield* SubscriptionRef.set(source, healthy);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseCallback, undefined);
      expect(yield* Queue.take(heartbeats)).toStrictEqual(unhealthy.heartbeat);

      yield* TestClock.adjust("300 millis");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("rechecks the change interval after a colliding periodic callback", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const periodicStarted = yield* Deferred.make<void>();
      const releasePeriodic = yield* Deferred.make<void>();
      const source = yield* SubscriptionRef.make(healthy);
      const scope = yield* Scope.make("sequential");
      let readyCallbacks = 0;
      yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.millis(301),
          dependenciesInterval: Duration.hours(1),
          changeInterval: Duration.millis(300),
          onHeartbeat: (heartbeat) => {
            readyCallbacks += 1;
            return readyCallbacks === 2
              ? Deferred.succeed(periodicStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releasePeriodic)),
                  Effect.andThen(Queue.offer(heartbeats, heartbeat)),
                  Effect.asVoid,
                )
              : Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid);
          },
          onDependenciesUpdate: () => Effect.void,
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: SubscriptionRef.changes(source),
        },
      );
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);

      yield* TestClock.adjust("301 millis");
      yield* Deferred.await(periodicStarted);
      yield* SubscriptionRef.set(source, unhealthy);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releasePeriodic, undefined);
      expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);

      yield* TestClock.adjust("299 millis");
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);
      yield* TestClock.adjust("1 millis");
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(unhealthy.heartbeat);
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("makes Stopping terminal across periodic and semantic emissions", () =>
    Effect.gen(function* () {
      const heartbeats = yield* Queue.unbounded<RuntimeHeartbeat>();
      const periodicStarted = yield* Deferred.make<void>();
      const periodicInterrupted = yield* Deferred.make<void>();
      const dependenciesStarted = yield* Deferred.make<void>();
      const source = yield* SubscriptionRef.make(healthy);
      const scope = yield* Scope.make("sequential");
      let callbacks = 0;
      let dependencyCallbacks = 0;
      const reporter = yield* makeRuntimeReporting(
        scope,
        {
          heartbeatInterval: Duration.millis(100),
          dependenciesInterval: Duration.millis(100),
          changeInterval: Duration.millis(10),
          onHeartbeat: (heartbeat) => {
            callbacks += 1;
            return callbacks === 2
              ? Deferred.succeed(periodicStarted, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(Deferred.succeed(periodicInterrupted, undefined)),
                )
              : Queue.offer(heartbeats, heartbeat).pipe(Effect.asVoid);
          },
          onDependenciesUpdate: () => {
            dependencyCallbacks += 1;
            return Deferred.succeed(dependenciesStarted, undefined).pipe(Effect.asVoid);
          },
        },
        {
          snapshot: SubscriptionRef.get(source),
          changes: SubscriptionRef.changes(source),
        },
      );
      yield* Effect.yieldNow;
      expect(yield* Queue.take(heartbeats)).toStrictEqual(healthy.heartbeat);
      yield* TestClock.adjust("100 millis");
      yield* Deferred.await(periodicStarted);
      yield* Deferred.await(dependenciesStarted);
      const dependencyCallbacksBeforeStopping = dependencyCallbacks;

      const stopping = yield* reporter.stopping.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      yield* SubscriptionRef.set(source, unhealthy);
      yield* Fiber.join(stopping);
      yield* Deferred.await(periodicInterrupted);
      expect(yield* Queue.take(heartbeats)).toStrictEqual({ status: "Stopping", problems: [] });

      yield* TestClock.adjust("1 hour");
      yield* Effect.yieldNow;
      expect(Option.isNone(yield* Queue.poll(heartbeats))).toBe(true);
      expect(dependencyCallbacks).toBe(dependencyCallbacksBeforeStopping);
      yield* Scope.close(scope, Exit.void);
    }),
  );
});
