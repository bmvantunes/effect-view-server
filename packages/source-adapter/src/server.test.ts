import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Chunk,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Result,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import { SourceAdapter } from "./index";
import {
  decodeSourceToolkitUpsert,
  isSourceApplicationTransition,
  isSourceApplicationStateRegistration,
  isSourceMaintenanceOperation,
  makeSourceApplicationTransition,
  makeSourceDelete,
  makeSourceDelivery,
  makeSourceItemRejection,
  makeSourceTransitionDelivery,
  makeSourceUpsert,
  markSourceToolkit,
  resolveSourceApplicationTransition,
  resolveSourceApplicationStateRegistration,
  resolveSourceMaintenanceOperation,
} from "./internal";
import type {
  SourceApplicationExit,
  SourceApplicationTransition,
  SourceDelivery,
  SourceMaintenanceResult,
  SourceMutation,
  SourceSettlement,
  SourceToolkit,
} from "./index";
import { SourceAdapterServer, type SourceAdapterServerLifecycle } from "./server";

const Failure = Schema.TaggedStruct("ServerFixtureFailure", {
  message: Schema.String,
});
const Metrics = Schema.Struct({
  active: Schema.Boolean,
});
const Location = Schema.Struct({
  offset: Schema.BigInt,
});

const Adapter = SourceAdapter.make({
  identity: { name: "server-fixture" },
  failure: Failure,
  materialized: {
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
    }>(),
  },
  leased: undefined,
});

const StatefulAdapter = SourceAdapter.make({
  identity: { name: "stateful-server-fixture" },
  failure: Failure,
  materialized: {
    applicationState: "required",
    metrics: Metrics,
    rejectionLocation: Location,
    definitionOptions: SourceAdapter.definitionOptions<{
      readonly label: string;
    }>(),
  },
  leased: undefined,
});

function toolkitDelivery(
  mutations: Chunk.NonEmptyChunk<SourceMutation<{ readonly id: string }>>,
  settlement?: SourceSettlement<typeof Failure.Type>,
): Effect.Effect<SourceDelivery<{ readonly id: string }, typeof Failure.Type, never, "orders">>;
function toolkitDelivery(
  mutation: SourceMutation<{ readonly id: string }>,
  settlement: SourceSettlement<typeof Failure.Type> | undefined,
  transition: SourceApplicationTransition<"orders">,
): Effect.Effect<SourceDelivery<{ readonly id: string }, typeof Failure.Type, never, "orders">>;
function toolkitDelivery(
  mutationsOrMutation:
    | Chunk.NonEmptyChunk<SourceMutation<{ readonly id: string }>>
    | SourceMutation<{ readonly id: string }>,
  settlement?: SourceSettlement<typeof Failure.Type>,
  transition?: SourceApplicationTransition<"orders">,
): Effect.Effect<SourceDelivery<{ readonly id: string }, typeof Failure.Type, never, "orders">> {
  return transition === undefined && Chunk.isChunk(mutationsOrMutation)
    ? Effect.succeed(makeSourceDelivery(mutationsOrMutation, settlement))
    : transition !== undefined && !Chunk.isChunk(mutationsOrMutation)
      ? Effect.succeed(makeSourceTransitionDelivery(mutationsOrMutation, settlement, transition))
      : Effect.die(new TypeError("Invalid fixture delivery shape."));
}

const toolkit: SourceToolkit<
  { readonly id: string },
  typeof Failure.Type,
  { readonly offset: bigint },
  never,
  "orders"
> = markSourceToolkit({
  topic: "orders",
  upsert: (row) => Effect.succeed(makeSourceUpsert<{ readonly id: string }>(row)),
  decodeUpsert: (row: unknown) => Effect.succeed(makeSourceUpsert({ id: String(row) })),
  delete: (id: string) => Effect.succeed(makeSourceDelete(id)),
  delivery: toolkitDelivery,
  reject: (input) => Effect.succeed(makeSourceItemRejection(input)),
});

const nominalClone = <Value extends object>(
  value: Value,
  overrides: Readonly<Record<string, unknown>>,
): Value => {
  const clone: Value = Object.create(Object.getPrototypeOf(value));
  for (const property of Reflect.ownKeys(value)) {
    const descriptor = Reflect.getOwnPropertyDescriptor(value, property);
    if (descriptor === undefined) {
      continue;
    }
    const next =
      typeof property === "symbol" &&
      "value" in descriptor &&
      typeof descriptor.value === "function"
        ? {
            ...descriptor,
            value: () => clone,
          }
        : typeof property === "string" &&
            Object.hasOwn(overrides, property) &&
            "value" in descriptor
          ? {
              ...descriptor,
              value: overrides[property],
            }
          : descriptor;
    Object.defineProperty(clone, property, next);
  }
  return Object.freeze(clone);
};

const hostileNominalClone = <Value extends object>(
  value: Value,
  propertyToThrow: string,
): Value => {
  const clone: Value = Object.create(Object.getPrototypeOf(value));
  for (const property of Reflect.ownKeys(value)) {
    const descriptor = Reflect.getOwnPropertyDescriptor(value, property);
    if (descriptor === undefined) {
      continue;
    }
    const next =
      typeof property === "symbol" &&
      "value" in descriptor &&
      typeof descriptor.value === "function"
        ? {
            ...descriptor,
            value: () => clone,
          }
        : property === propertyToThrow
          ? {
              configurable: false,
              enumerable: descriptor.enumerable === true,
              get() {
                throw new Error(`hostile ${propertyToThrow}`);
              },
            }
          : descriptor;
    Object.defineProperty(clone, property, next);
  }
  return Object.freeze(clone);
};

const invokeUnknownMethod = (
  receiver: unknown,
  property: PropertyKey,
  args: ReadonlyArray<unknown>,
): unknown => {
  if (typeof receiver !== "object" || receiver === null) {
    throw new TypeError("Expected method receiver to be an object.");
  }
  const method = Reflect.get(receiver, property);
  if (typeof method !== "function") {
    throw new TypeError("Expected property to be a function.");
  }
  return Reflect.apply(method, receiver, args);
};

describe("Source Adapter server SDK", () => {
  it.effect("owns synchronous application state, transitions, and maintenance operations", () =>
    Effect.gen(function* () {
      const mutableCancellationIds = ["obsolete:1"];
      let activeTransitionLeases = 0;
      let releaseCount = 0;
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: { readonly delta: number }) => state + command.delta,
        cancelledMaintenanceWorkIds: (_state, command) =>
          command.delta === 1 ? mutableCancellationIds : [],
        acquireTransition: () =>
          Effect.sync(() => {
            activeTransitionLeases += 1;
            return () => {
              activeTransitionLeases -= 1;
              releaseCount += 1;
            };
          }),
        metrics: (state) => ({ value: state }),
        runDueSweep: (input) =>
          Effect.gen(function* () {
            const operations = [
              input.operation({
                id: "success",
                workId: "success:1",
                isCurrent: (state) => state === 3,
                onSuccess: { delta: 4 },
                onFailure: () => ({ delta: 0 }),
                onStale: { delta: 0 },
              }),
              input.operation({
                id: "failure",
                workId: "failure:1",
                isCurrent: () => true,
                onSuccess: { delta: 0 },
                onFailure: () => ({ delta: 8 }),
                onStale: { delta: 0 },
              }),
              input.operation({
                id: "stale",
                workId: "stale:1",
                isCurrent: () => false,
                onSuccess: { delta: 0 },
                onFailure: () => ({ delta: 0 }),
                onStale: { delta: 16 },
              }),
            ] as const;
            yield* Effect.forEach(operations, input.execute, {
              discard: true,
            });
            return {
              operations: operations.length,
            };
          }),
      });
      expect(() =>
        Reflect.apply(SourceAdapterServer.applicationState, undefined, [
          {
            sweepIntervalNanos: 0n,
            initialState: () => undefined,
            reduce: () => undefined,
            metrics: () => undefined,
            runDueSweep: () => Effect.void,
          },
        ]),
      ).toThrow(
        "Source Application State registration requires exact synchronous state capabilities and a positive finite sweep interval.",
      );
      expect(() =>
        Reflect.apply(SourceAdapterServer.applicationState, undefined, [
          {
            sweepIntervalNanos: 1,
            initialState: () => undefined,
            reduce: () => undefined,
            metrics: () => undefined,
            runDueSweep: () => Effect.void,
          },
        ]),
      ).toThrow(
        "Source Application State registration requires exact synchronous state capabilities and a positive finite sweep interval.",
      );
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      const registrationInternal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      const binding = {
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      } as const;
      registrationInternal.bind(binding);
      expect(() => registrationInternal.bind(binding)).toThrow(
        "Source Application State registration cannot bind one logical lifetime twice.",
      );
      const module = registration.forLifetime(lifetimeScope, "orders");
      expect(isSourceApplicationStateRegistration(registration)).toBe(true);
      expect(() => registration.forLifetime(lifetimeScope, "payments")).toThrow(
        'Source Application State is bound to topic "orders", not "payments".',
      );
      const stateContract = Option.getOrThrow(
        Option.fromUndefinedOr(Reflect.ownKeys(module).find((key) => typeof key === "symbol")),
      );
      expect(invokeUnknownMethod(module, stateContract, [42])).toBe(42);
      const prepared = yield* module
        .prepare({ delta: 1 })
        .pipe(Effect.provideService(Scope.Scope, attemptScope));
      const transition = prepared.transition;
      const transitionInternal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(transition)),
      );
      mutableCancellationIds[0] = "hostile-replacement";
      mutableCancellationIds.push("hostile-addition");
      expect(isSourceApplicationTransition(transition)).toBe(true);
      expect(transitionInternal.topic).toBe("orders");
      expect(transitionInternal.cancelledMaintenanceWorkIds).toStrictEqual(["obsolete:1"]);
      expect(transitionInternal.lifetimeIdentity).toBe(
        registrationInternal.lifetimeIdentity(lifetimeScope),
      );
      expect(activeTransitionLeases).toBe(1);
      transitionInternal.apply();
      let settlementObservedActiveLeases = -1;
      const settlement = () => {
        settlementObservedActiveLeases = activeTransitionLeases;
        return Effect.void;
      };
      yield* settlement();
      const noChange = yield* module
        .prepare({ delta: 0 })
        .pipe(Effect.provideService(Scope.Scope, attemptScope));
      expect(noChange.transition.topic).toBe("orders");
      const second = yield* module
        .prepare({ delta: 2 })
        .pipe(Effect.provideService(Scope.Scope, attemptScope));
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(second.transition)),
      ).apply();
      yield* prepared.release;
      yield* noChange.release;
      yield* second.release;
      expect({
        activeTransitionLeases,
        releaseCount,
        settlementObservedActiveLeases,
      }).toStrictEqual({
        activeTransitionLeases: 0,
        releaseCount: 3,
        settlementObservedActiveLeases: 0,
      });

      let execution = 0;
      const actions: ReadonlyArray<
        (
          internal: NonNullable<ReturnType<typeof resolveSourceMaintenanceOperation>>,
        ) => SourceMaintenanceResult
      > = [
        (internal: NonNullable<ReturnType<typeof resolveSourceMaintenanceOperation>>) => {
          expect(internal.isCurrent()).toBe(true);
          internal.onSuccess();
          return { _tag: "Applied", exit: Exit.void };
        },
        (internal: NonNullable<ReturnType<typeof resolveSourceMaintenanceOperation>>) => {
          const exit: SourceApplicationExit = Exit.fail({
            _tag: "InvalidTopicRow",
            topic: "orders",
            message: "fixture",
          });
          internal.onFailure(exit);
          return { _tag: "Applied", exit };
        },
        (internal: NonNullable<ReturnType<typeof resolveSourceMaintenanceOperation>>) => {
          expect(internal.isCurrent()).toBe(false);
          internal.onStale();
          return { _tag: "Stale" };
        },
      ];
      const outcome = yield* module.runDueSweep(1_000n, (operation) =>
        Effect.sync(() => {
          expect(isSourceMaintenanceOperation(operation)).toBe(true);
          const internal = Option.getOrThrow(
            Option.fromUndefinedOr(resolveSourceMaintenanceOperation(operation)),
          );
          expect(internal.lifetimeIdentity).toBe(
            registrationInternal.lifetimeIdentity(lifetimeScope),
          );
          const action = Option.getOrThrow(Option.fromUndefinedOr(actions[execution]));
          const result = action(internal);
          execution += 1;
          return result;
        }),
      );
      const internalOutcome = yield* registrationInternal.runDueSweep(lifetimeScope, 2_000n, () =>
        Effect.succeed({ _tag: "Inactive" }),
      );

      expect({
        frozen: Object.isFrozen(module),
        metrics: module.metrics(),
        outcome,
        internalOutcome,
      }).toStrictEqual({
        frozen: true,
        metrics: { value: 31 },
        outcome: { operations: 3 },
        internalOutcome: { operations: 3 },
      });
      const fallbackReleased = yield* module
        .prepare({ delta: 0 })
        .pipe(Effect.provideService(Scope.Scope, attemptScope));
      expect(fallbackReleased.transition.topic).toBe("orders");
      expect(activeTransitionLeases).toBe(1);
      yield* Scope.close(attemptScope, Exit.void);
      expect({
        activeTransitionLeases,
        releaseCount,
      }).toStrictEqual({
        activeTransitionLeases: 0,
        releaseCount: 4,
      });
      registrationInternal.unbind(lifetimeScope);
      expect(() => registration.forLifetime(lifetimeScope, "orders")).toThrow(
        "Source Application State registration is not bound to this logical lifetime.",
      );
      expect(() => registrationInternal.lifetimeIdentity(lifetimeScope)).toThrow(
        "Source Application State registration is not bound to this logical lifetime.",
      );
      const unboundSweep = registrationInternal.runDueSweep(lifetimeScope, 2_000n, () =>
        Effect.succeed({ _tag: "Inactive" }),
      );
      const unboundCause = yield* unboundSweep.pipe(Effect.sandbox, Effect.flip);
      expect(Result.getOrThrow(Cause.findDefect(unboundCause))).toStrictEqual(
        new TypeError(
          "Source Application State registration is not bound to this logical lifetime.",
        ),
      );
      yield* Scope.close(lifetimeScope, Exit.void);

      const defaultLeaseRegistration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: { readonly delta: number }) => state + command.delta,
        metrics: (state) => state,
        runDueSweep: () => Effect.void,
      });
      const defaultLeaseScope = yield* Scope.make();
      const defaultAttemptScope = yield* Scope.make();
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(defaultLeaseRegistration)),
      ).bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope: defaultLeaseScope,
        target: { _tag: "Materialized" },
      });
      const defaultLeaseModule = defaultLeaseRegistration.forLifetime(defaultLeaseScope, "orders");
      const defaultLeaseTransition = yield* defaultLeaseModule
        .prepare({ delta: 1 })
        .pipe(Effect.provideService(Scope.Scope, defaultAttemptScope));
      Option.getOrThrow(
        Option.fromUndefinedOr(
          resolveSourceApplicationTransition(defaultLeaseTransition.transition),
        ),
      ).apply();
      yield* defaultLeaseTransition.release;
      yield* Scope.close(defaultAttemptScope, Exit.void);
      yield* Scope.close(defaultLeaseScope, Exit.void);
      expect(defaultLeaseModule.metrics()).toBe(1);
    }),
  );

  it.effect("attempts every active transition release when attempt cleanup defects", () =>
    Effect.gen(function* () {
      const releaseAttempts: Array<number> = [];
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: number) => state + command,
        acquireTransition: (_state, command) =>
          Effect.succeed(() => {
            releaseAttempts.push(command);
            throw new Error(`release ${command}`);
          }),
        metrics: (state) => state,
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      ).bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      yield* module.prepare(1).pipe(Effect.provideService(Scope.Scope, attemptScope));
      yield* module.prepare(2).pipe(Effect.provideService(Scope.Scope, attemptScope));

      const cause = yield* Scope.close(attemptScope, Exit.void).pipe(Effect.sandbox, Effect.flip);

      expect(releaseAttempts).toStrictEqual([1, 2]);
      expect(Cause.hasDies(cause)).toBe(true);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("does not acquire a transition after its attempt scope has closed", () =>
    Effect.gen(function* () {
      let acquisitions = 0;
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: number) => state + command,
        acquireTransition: () => {
          acquisitions += 1;
          return Effect.succeed(() => undefined);
        },
        metrics: (state) => state,
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      ).bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      yield* Scope.close(attemptScope, Exit.void);

      const exit = yield* Effect.exit(
        module.prepare(1).pipe(Effect.provideService(Scope.Scope, attemptScope)),
      );

      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(acquisitions).toBe(0);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("releases a transition acquired while its attempt scope closes", () =>
    Effect.gen(function* () {
      const acquisitionStarted = yield* Deferred.make<void>();
      const allowAcquisition = yield* Deferred.make<void>();
      let releases = 0;
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: number) => state + command,
        acquireTransition: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(acquisitionStarted, undefined);
            yield* Deferred.await(allowAcquisition);
            return () => {
              releases += 1;
            };
          }),
        metrics: (state) => state,
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      ).bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const preparing = yield* module
        .prepare(1)
        .pipe(Effect.provideService(Scope.Scope, attemptScope), Effect.forkChild);
      yield* Deferred.await(acquisitionStarted);

      yield* Scope.close(attemptScope, Exit.void);
      yield* Deferred.succeed(allowAcquisition, undefined);
      const exit = yield* Fiber.await(preparing);

      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(releases).toBe(1);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("registers an acquired transition before honoring a pending interruption", () =>
    Effect.gen(function* () {
      const allowPermit = yield* Deferred.make<void>();
      const permitAcquired = yield* Deferred.make<void>();
      const allowTransfer = yield* Deferred.make<void>();
      let releases = 0;
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: number) => state + command,
        acquireTransition: () =>
          Effect.uninterruptibleMask(() =>
            Effect.gen(function* () {
              yield* Effect.interruptible(Deferred.await(allowPermit));
              yield* Deferred.succeed(permitAcquired, undefined);
              yield* Deferred.await(allowTransfer);
              return () => {
                releases += 1;
              };
            }),
          ),
        metrics: (state) => state,
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      ).bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const preparing = yield* module
        .prepare(1)
        .pipe(Effect.provideService(Scope.Scope, attemptScope), Effect.forkChild);
      yield* Deferred.succeed(allowPermit, undefined);
      yield* Deferred.await(permitAcquired);

      preparing.interruptUnsafe();
      yield* Deferred.succeed(allowTransfer, undefined);
      const exit = yield* Fiber.await(preparing);
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(releases).toBe(0);

      yield* Scope.close(attemptScope, Exit.void);
      expect(releases).toBe(1);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("allows a conforming adapter permit wait to interrupt before acquisition", () =>
    Effect.gen(function* () {
      const permitWaitStarted = yield* Deferred.make<void>();
      const permitAvailable = yield* Deferred.make<void>();
      let acquired = false;
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: number) => state + command,
        acquireTransition: () =>
          Effect.uninterruptibleMask(() =>
            Effect.gen(function* () {
              yield* Deferred.succeed(permitWaitStarted, undefined);
              yield* Effect.interruptible(Deferred.await(permitAvailable));
              acquired = true;
              return () => undefined;
            }),
          ),
        metrics: (state) => state,
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      ).bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const preparing = yield* module
        .prepare(1)
        .pipe(Effect.provideService(Scope.Scope, attemptScope), Effect.forkChild);
      yield* Deferred.await(permitWaitStarted);

      yield* Fiber.interrupt(preparing);
      const exit = yield* Fiber.await(preparing);

      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(acquired).toBe(false);
      yield* Scope.close(attemptScope, Exit.void);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it("rejects malformed or asynchronous application state capabilities at construction", () => {
    const validInput = () => ({
      sweepIntervalNanos: 1_000n,
      initialState: () => 0,
      reduce: (state: number) => state,
      metrics: (state: number) => state,
      runDueSweep: () => Effect.void,
    });
    const missingCapability = validInput();
    Reflect.deleteProperty(missingCapability, "runDueSweep");
    const surplusCapability = {
      ...validInput(),
      select: () => 0,
    };
    const accessorCapability = validInput();
    Object.defineProperty(accessorCapability, "reduce", {
      enumerable: true,
      get: () => (state: number) => state,
    });
    const symbolicCapability = validInput();
    Object.defineProperty(symbolicCapability, Symbol("surplus"), {
      enumerable: true,
      value: undefined,
    });
    const hiddenCapability = validInput();
    Object.defineProperty(hiddenCapability, "metrics", {
      enumerable: false,
      value: (state: number) => state,
    });
    const throwingOwnKeys = new Proxy(validInput(), {
      ownKeys: () => {
        throw new Error("hostile ownKeys");
      },
    });
    const throwingDescriptor = new Proxy(validInput(), {
      getOwnPropertyDescriptor: () => {
        throw new Error("hostile descriptor");
      },
    });
    const hostileReducer = new Proxy((state: number) => state, {
      get: (_target, property, receiver) => {
        if (property === "constructor") {
          throw new Error("hostile constructor");
        }
        return Reflect.get(_target, property, receiver);
      },
    });
    const asyncReducer = async (state: number) => Promise.resolve(state);
    const asyncGeneratorReducer = async function* (state: number) {
      yield state;
    };
    const invalidInputs = [
      missingCapability,
      surplusCapability,
      accessorCapability,
      symbolicCapability,
      hiddenCapability,
      throwingOwnKeys,
      throwingDescriptor,
      {
        ...validInput(),
        sweepIntervalNanos: 1,
      },
      {
        ...validInput(),
        initialState: undefined,
      },
      {
        ...validInput(),
        reduce: hostileReducer,
      },
      {
        ...validInput(),
        reduce: asyncReducer,
      },
      {
        ...validInput(),
        reduce: asyncGeneratorReducer,
      },
      {
        ...validInput(),
        cancelledMaintenanceWorkIds: 1,
      },
      {
        ...validInput(),
        acquireTransition: 1,
      },
      {
        ...validInput(),
        metrics: undefined,
      },
      {
        ...validInput(),
        runDueSweep: undefined,
      },
    ];

    for (const input of invalidInputs) {
      expect(() => Reflect.apply(SourceAdapterServer.applicationState, undefined, [input])).toThrow(
        "Source Application State registration requires exact synchronous state capabilities and a positive finite sweep interval.",
      );
    }
  });

  it.effect("snapshots application-state capabilities at registration", () =>
    Effect.gen(function* () {
      let originalReductions = 0;
      let replacementReductions = 0;
      const input = {
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, command: { readonly delta: number }) => {
          originalReductions += 1;
          return state + command.delta;
        },
        metrics: (state: number) => state,
        runDueSweep: () => Effect.void,
      };
      const registration = SourceAdapterServer.applicationState(input);
      Reflect.set(input, "reduce", (state: number) => {
        replacementReductions += 1;
        return state + 100;
      });
      const lifetimeScope = yield* Scope.make();
      const registrationInternal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      registrationInternal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const prepared = yield* module
        .prepare({ delta: 2 })
        .pipe(Effect.provideService(Scope.Scope, lifetimeScope));
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(prepared.transition)),
      ).apply();

      expect({
        metrics: module.metrics(),
        originalReductions,
        replacementReductions,
      }).toStrictEqual({
        metrics: 2,
        originalReductions: 1,
        replacementReductions: 0,
      });
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("reads application state when a retention sweep Effect executes", () =>
    Effect.gen(function* () {
      let sweepInvocations = 0;
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number, increment: number) => state + increment,
        metrics: (state: number) => state,
        runDueSweep: (input) => {
          sweepInvocations += 1;
          return Effect.succeed(input.state);
        },
      });
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      const internal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      internal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const sweep = module.runDueSweep(1_000n, () => Effect.succeed({ _tag: "Inactive" }));
      const prepared = yield* module
        .prepare(1)
        .pipe(Effect.provideService(Scope.Scope, attemptScope));
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(prepared.transition)),
      ).apply();

      expect(sweepInvocations).toBe(0);
      expect(yield* sweep).toBe(1);
      expect(sweepInvocations).toBe(1);
      yield* Scope.close(attemptScope, Exit.void);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("accepts distinct frozen function-valued application states", () =>
    Effect.gen(function* () {
      type FunctionState = () => number;
      const makeState = (value: number): FunctionState => Object.freeze(() => value);
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: (): FunctionState => makeState(0),
        reduce: (state: FunctionState, _command: undefined): FunctionState =>
          makeState(state() + 1),
        metrics: (state: FunctionState) => state(),
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const internal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      internal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const prepared = yield* module
        .prepare(undefined)
        .pipe(Effect.provideService(Scope.Scope, lifetimeScope));
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(prepared.transition)),
      ).apply();

      expect(module.metrics()).toBe(1);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("accepts synchronous state and metrics with non-callable then fields", () =>
    Effect.gen(function* () {
      class ThenState {
        constructor(
          readonly then: string,
          readonly count: number,
        ) {
          Object.freeze(this);
        }
      }
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: (): ThenState => new ThenState("state", 0),
        reduce: (state: ThenState): ThenState => new ThenState(state.then, state.count + 1),
        metrics: (state: ThenState) => new ThenState("metrics", state.count),
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const internal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      internal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const prepared = yield* module
        .prepare({})
        .pipe(Effect.provideService(Scope.Scope, lifetimeScope));
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(prepared.transition)),
      ).apply();

      expect(module.metrics()).toStrictEqual(new ThenState("metrics", 1));
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it("rejects mutable or asynchronous initial state and asynchronous metrics at binding", () => {
    const callableThenState = () =>
      new Proxy(Object.freeze({ value: 0 }), {
        get: (target, property, receiver) =>
          property === "then" ? () => undefined : Reflect.get(target, property, receiver),
      });
    const registrations = [
      SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => ({ value: 0 }),
        reduce: (state: { readonly value: number }) => state,
        metrics: () => undefined,
        runDueSweep: () => Effect.void,
      }),
      Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: (): (() => void) => () => undefined,
          reduce: (state: () => void): (() => void) => state,
          metrics: () => undefined,
          runDueSweep: () => Effect.void,
        },
      ]),
      Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => Effect.succeed(0),
          reduce: (state: number) => state,
          metrics: () => undefined,
          runDueSweep: () => Effect.void,
        },
      ]),
      Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => Object.freeze(Promise.resolve(0)),
          reduce: (state: number) => state,
          metrics: () => undefined,
          runDueSweep: () => Effect.void,
        },
      ]),
      Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: callableThenState,
          reduce: (state: { readonly value: number }) => state,
          metrics: () => undefined,
          runDueSweep: () => Effect.void,
        },
      ]),
      Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => 0,
          reduce: (state: number) => state,
          metrics: () => Effect.void,
          runDueSweep: () => Effect.void,
        },
      ]),
      Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => 0,
          reduce: (state: number) => state,
          metrics: () => Promise.resolve(0),
          runDueSweep: () => Effect.void,
        },
      ]),
    ];
    const expectedMessages = [
      "Source Application State initial state must be immutable.",
      "Source Application State initial state must be immutable.",
      "Source Application State initial state must return a synchronous snapshot.",
      "Source Application State initial state must return a synchronous snapshot.",
      "Source Application State initial state must return a synchronous snapshot.",
      "Source Application State metrics must return a synchronous snapshot.",
      "Source Application State metrics must return a synchronous snapshot.",
    ];

    for (const [index, registration] of registrations.entries()) {
      const lifetimeScope = Effect.runSync(Scope.make());
      const internal = resolveSourceApplicationStateRegistration(registration);
      expect(internal).not.toBeUndefined();
      expect(() =>
        internal?.bind({
          topic: "orders",
          definition: undefined,
          lifetimeScope,
          target: { _tag: "Materialized" },
        }),
      ).toThrow(expectedMessages[index]);
    }
  });

  it.effect("rejects asynchronous metrics after application state transitions", () =>
    Effect.gen(function* () {
      const registration = Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => 0,
          reduce: () => 1,
          metrics: (state: number) => (state === 0 ? 0 : Effect.void),
          runDueSweep: () => Effect.void,
        },
      ]);
      const lifetimeScope = yield* Scope.make();
      const internal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      internal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = Reflect.apply(Reflect.get(registration, "forLifetime"), registration, [
        lifetimeScope,
        "orders",
      ]);
      const preparedEffect = invokeUnknownMethod(module, "prepare", [undefined]);
      if (!Effect.isEffect(preparedEffect)) {
        throw new TypeError("Expected prepare to return an Effect.");
      }
      const prepared = yield* preparedEffect.pipe(
        Effect.provideService(Scope.Scope, lifetimeScope),
        Effect.orDie,
      );
      if (typeof prepared !== "object" || prepared === null) {
        throw new TypeError("Expected prepare to return a transition.");
      }
      Option.getOrThrow(
        Option.fromUndefinedOr(
          resolveSourceApplicationTransition(Reflect.get(prepared, "transition")),
        ),
      ).apply();

      expect(() => invokeUnknownMethod(module, "metrics", [])).toThrow(
        "Source Application State metrics must return a synchronous snapshot.",
      );
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("observes rejected Promise-like metrics before rejecting the snapshot", () =>
    Effect.gen(function* () {
      let rejectionObserverInstalled = false;
      const rejectedMetrics = new Proxy(
        {},
        {
          get: (_target, property) =>
            property === "then"
              ? (_onSuccess: (value: never) => void, onFailure: (failure: Error) => void): void => {
                  rejectionObserverInstalled = true;
                  onFailure(new Error("hostile metrics rejection"));
                }
              : undefined,
        },
      );
      const registration = Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => 0,
          reduce: () => 1,
          metrics: (state: number) => (state === 0 ? 0 : rejectedMetrics),
          runDueSweep: () => Effect.void,
        },
      ]);
      const lifetimeScope = yield* Scope.make();
      const internal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      internal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = Reflect.apply(Reflect.get(registration, "forLifetime"), registration, [
        lifetimeScope,
        "orders",
      ]);
      const preparedEffect = invokeUnknownMethod(module, "prepare", [undefined]);
      if (!Effect.isEffect(preparedEffect)) {
        throw new TypeError("Expected prepare to return an Effect.");
      }
      const prepared = yield* preparedEffect.pipe(
        Effect.provideService(Scope.Scope, lifetimeScope),
        Effect.orDie,
      );
      if (typeof prepared !== "object" || prepared === null) {
        throw new TypeError("Expected prepare to return a transition.");
      }
      Option.getOrThrow(
        Option.fromUndefinedOr(
          resolveSourceApplicationTransition(Reflect.get(prepared, "transition")),
        ),
      ).apply();

      expect(() => invokeUnknownMethod(module, "metrics", [])).toThrow(
        "Source Application State metrics must return a synchronous snapshot.",
      );
      yield* Effect.yieldNow;
      expect(rejectionObserverInstalled).toBe(true);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("rejects hostile asynchronous application-state capability results", () =>
    Effect.gen(function* () {
      const cancelledRegistration = Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => 0,
          reduce: (state: number) => state,
          cancelledMaintenanceWorkIds: () => Promise.resolve([]),
          metrics: () => 0,
          runDueSweep: () => Effect.void,
        },
      ]);
      const cancelledLifetimeScope = yield* Scope.make();
      const cancelledAttemptScope = yield* Scope.make();
      const cancelledInternal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(cancelledRegistration)),
      );
      cancelledInternal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope: cancelledLifetimeScope,
        target: { _tag: "Materialized" },
      });
      const cancelledModule = Reflect.apply(
        Reflect.get(cancelledRegistration, "forLifetime"),
        cancelledRegistration,
        [cancelledLifetimeScope, "orders"],
      );
      const cancelledPreparation = invokeUnknownMethod(cancelledModule, "prepare", [undefined]);
      if (!Effect.isEffect(cancelledPreparation)) {
        throw new TypeError("Expected cancellation preparation to return an Effect.");
      }
      const cancelledExit = yield* Effect.exit(
        cancelledPreparation.pipe(Effect.provideService(Scope.Scope, cancelledAttemptScope)),
      );
      expect(
        Exit.isFailure(cancelledExit)
          ? Result.getOrThrow(Cause.findDefect(cancelledExit.cause))
          : undefined,
      ).toStrictEqual(
        new TypeError(
          "Source Application State cancelled maintenance work IDs must return synchronously.",
        ),
      );

      const acquisitionRegistration = Reflect.apply(
        SourceAdapterServer.applicationState,
        undefined,
        [
          {
            sweepIntervalNanos: 1_000n,
            initialState: () => 0,
            reduce: (state: number) => state,
            acquireTransition: () => Promise.resolve(() => undefined),
            metrics: () => 0,
            runDueSweep: () => Effect.void,
          },
        ],
      );
      const acquisitionLifetimeScope = yield* Scope.make();
      const acquisitionAttemptScope = yield* Scope.make();
      const acquisitionInternal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(acquisitionRegistration)),
      );
      acquisitionInternal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope: acquisitionLifetimeScope,
        target: { _tag: "Materialized" },
      });
      const acquisitionModule = Reflect.apply(
        Reflect.get(acquisitionRegistration, "forLifetime"),
        acquisitionRegistration,
        [acquisitionLifetimeScope, "orders"],
      );
      const acquisitionPreparation = invokeUnknownMethod(acquisitionModule, "prepare", [undefined]);
      if (!Effect.isEffect(acquisitionPreparation)) {
        throw new TypeError("Expected acquisition preparation to return an Effect.");
      }
      const acquisitionExit = yield* Effect.exit(
        acquisitionPreparation.pipe(Effect.provideService(Scope.Scope, acquisitionAttemptScope)),
      );
      expect(
        Exit.isFailure(acquisitionExit)
          ? Result.getOrThrow(Cause.findDefect(acquisitionExit.cause))
          : undefined,
      ).toStrictEqual(
        new TypeError("Source Application State transition acquisition must return an Effect."),
      );

      const sweepRegistration = Reflect.apply(SourceAdapterServer.applicationState, undefined, [
        {
          sweepIntervalNanos: 1_000n,
          initialState: () => 0,
          reduce: (state: number) => state,
          metrics: () => 0,
          runDueSweep: () => Promise.resolve({ attempted: 0 }),
        },
      ]);
      const sweepLifetimeScope = yield* Scope.make();
      const sweepInternal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(sweepRegistration)),
      );
      sweepInternal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope: sweepLifetimeScope,
        target: { _tag: "Materialized" },
      });
      const sweepModule = Reflect.apply(
        Reflect.get(sweepRegistration, "forLifetime"),
        sweepRegistration,
        [sweepLifetimeScope, "orders"],
      );
      const sweep = invokeUnknownMethod(sweepModule, "runDueSweep", [
        1n,
        () => Effect.succeed({ _tag: "Inactive" }),
      ]);
      if (!Effect.isEffect(sweep)) {
        throw new TypeError("Expected retention sweep to return an Effect.");
      }
      const sweepExit = yield* Effect.exit(sweep);
      expect(
        Exit.isFailure(sweepExit)
          ? Result.getOrThrow(Cause.findDefect(sweepExit.cause))
          : undefined,
      ).toStrictEqual(
        new TypeError("Source Application State retention sweep must return an Effect."),
      );

      yield* Scope.close(cancelledAttemptScope, Exit.void);
      yield* Scope.close(cancelledLifetimeScope, Exit.void);
      yield* Scope.close(acquisitionAttemptScope, Exit.void);
      yield* Scope.close(acquisitionLifetimeScope, Exit.void);
      yield* Scope.close(sweepLifetimeScope, Exit.void);
    }),
  );

  it("rejects Effect, Promise, and mutable-state application reducers", () => {
    const effectRegistration = Reflect.apply(SourceAdapterServer.applicationState, undefined, [
      {
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: () => Effect.succeed(1),
        metrics: () => undefined,
        runDueSweep: () => Effect.void,
      },
    ]);
    const reducerInputs = [
      {
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: () => Promise.resolve(1),
        metrics: () => undefined,
        runDueSweep: () => Effect.void,
      },
      {
        sweepIntervalNanos: 1_000n,
        initialState: () => Object.freeze({ value: 0 }),
        reduce: () => ({ value: 1 }),
        metrics: () => undefined,
        runDueSweep: () => Effect.void,
      },
      {
        sweepIntervalNanos: 1_000n,
        initialState: () => Object.freeze(() => undefined),
        reduce: () => () => undefined,
        metrics: () => undefined,
        runDueSweep: () => Effect.void,
      },
    ];
    const registrations = [
      effectRegistration,
      ...reducerInputs.map((input) =>
        Reflect.apply(SourceAdapterServer.applicationState, undefined, [input]),
      ),
    ];
    const bind = (registration: object, lifetimeScope: Scope.Scope) =>
      Reflect.apply(
        Reflect.get(
          Reflect.apply(resolveSourceApplicationStateRegistration, undefined, [registration]),
          "bind",
        ),
        undefined,
        [
          {
            topic: "orders",
            definition: undefined,
            lifetimeScope,
            target: { _tag: "Materialized" },
          },
        ],
      );
    const applyPreparedTransition = (module: unknown, scope: Scope.Scope): void => {
      const preparedEffect = invokeUnknownMethod(module, "prepare", [{}]);
      if (!Effect.isEffect(preparedEffect)) {
        throw new TypeError("Expected prepare to return an Effect.");
      }
      const runInScope = <Value, Error>(effect: Effect.Effect<Value, Error, Scope.Scope>): Value =>
        Effect.runSync(effect.pipe(Effect.provide(Context.make(Scope.Scope, scope)), Effect.orDie));
      const prepared = runInScope(preparedEffect);
      if (typeof prepared !== "object" || prepared === null) {
        throw new TypeError("Expected prepare to return a transition.");
      }
      const internal = resolveSourceApplicationTransition(Reflect.get(prepared, "transition"));
      if (internal === undefined) {
        throw new TypeError("Expected a nominal transition.");
      }
      internal.apply();
    };

    for (const registration of registrations) {
      const scope = Effect.runSync(Scope.make());
      bind(registration, scope);
      const module = Reflect.apply(Reflect.get(registration, "forLifetime"), registration, [
        scope,
        "orders",
      ]);
      expect(() => applyPreparedTransition(module, scope)).toThrow(
        "Source Application State reducer must return an immutable state synchronously.",
      );
    }
  });

  it.effect("accepts an unchanged frozen reducer state as an immutable no-op", () =>
    Effect.gen(function* () {
      const initialState = Object.freeze({ value: 1 });
      const registration = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => initialState,
        reduce: (state: { readonly value: number }) => state,
        metrics: (state) => ({ value: state.value }),
        runDueSweep: () => Effect.void,
      });
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      const internal = Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationStateRegistration(registration)),
      );
      internal.bind({
        topic: "orders",
        definition: undefined,
        lifetimeScope,
        target: { _tag: "Materialized" },
      });
      const module = registration.forLifetime(lifetimeScope, "orders");
      const prepared = yield* module
        .prepare(undefined)
        .pipe(Effect.provideService(Scope.Scope, attemptScope));
      Option.getOrThrow(
        Option.fromUndefinedOr(resolveSourceApplicationTransition(prepared.transition)),
      ).apply();

      expect(module.metrics()).toStrictEqual({ value: 1 });
      yield* prepared.release;
      yield* Scope.close(attemptScope, Exit.void);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it("collects only nominal definitions belonging to the requested Adapter", () => {
    const definition = Adapter.materializedSource({ label: "orders" });
    const otherAdapter = SourceAdapter.make({
      identity: { name: "other-server-fixture" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: undefined,
    });
    const otherDefinition = otherAdapter.materializedSource(undefined);
    let sourceAccessorCalls = 0;
    const accessorTopic = {};
    Object.defineProperty(accessorTopic, "source", {
      enumerable: true,
      get: () => {
        sourceAccessorCalls += 1;
        return definition;
      },
    });
    let topicAccessorCalls = 0;
    const topics = {
      orders: { source: definition },
      other: { source: otherDefinition },
      absent: {},
      sourceFree: { schema: "source-free" },
      primitive: "source-free",
      accessor: accessorTopic,
      forged: { source: { adapter: Adapter } },
    };
    Object.defineProperty(topics, "topicAccessor", {
      enumerable: true,
      get: () => {
        topicAccessorCalls += 1;
        return { source: definition };
      },
    });
    Object.defineProperty(topics, "hidden", {
      enumerable: false,
      value: { source: definition },
    });
    const definitions = SourceAdapterServer.definitions(
      {
        topics,
      },
      Adapter,
    );

    expect(definitions).toStrictEqual([
      {
        topic: "orders",
        definition,
      },
    ]);
    expect(Object.isFrozen(definitions)).toBe(true);
    expect(sourceAccessorCalls).toBe(0);
    expect(topicAccessorCalls).toBe(0);
  });

  it.effect("keeps attempt resources in the caller attempt Scope", () =>
    Effect.gen(function* () {
      let finalized = 0;
      class ReportingDependency extends Context.Service<
        ReportingDependency,
        { readonly endpoint: string }
      >()("@effect-view-server/source-adapter/test/ReportingDependency") {}
      const applicationState = SourceAdapterServer.applicationState({
        sweepIntervalNanos: 1_000n,
        initialState: () => 0,
        reduce: (state: number) => state,
        metrics: (state) => state,
        runDueSweep: () => Effect.void,
      });
      const adapterLayer = SourceAdapterServer.make(StatefulAdapter, {
        reporting: {
          dependencies: (input) =>
            Effect.gen(function* () {
              const dependency = yield* ReportingDependency;
              return [{ target: input.definition.label, endpoints: [dependency.endpoint] }];
            }),
          classifyFailure: () => ({ problem: "dependency", targets: ["orders"] }),
        },
        materialized: {
          applicationState,
          initialLaneIds: () => ["materialized", "sibling"],
          acquire: (input) =>
            Effect.gen(function* () {
              yield* Scope.addFinalizer(
                yield* Effect.scope,
                Effect.sync(() => {
                  finalized += 1;
                }),
              );
              const mutation = yield* input.toolkit.delete("a");
              yield* decodeSourceToolkitUpsert(input.toolkit, { id: "decoded" });
              const event = yield* input.toolkit.delivery(Chunk.of(mutation));
              const failure = yield* StatefulAdapter.failure({
                _tag: "ServerFixtureFailure",
                message: "rejected",
              }).pipe(Effect.orDie);
              const rejection = yield* input.toolkit.reject({
                failure,
                location: { offset: 1n },
                rejectedAtNanos: 2n,
              });
              const events = Stream.make(event, rejection);
              return SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "materialized",
                  events,
                }),
                SourceAdapterServer.lane({
                  id: "sibling",
                  events,
                }),
              ]);
            }),
          metrics: () =>
            ReportingDependency.pipe(
              Effect.map((dependency) => ({ active: dependency.endpoint.length > 0 })),
            ),
          retry: Schedule.recurs(0),
        },
      }).pipe(
        Layer.provide(
          Layer.succeed(ReportingDependency)({
            endpoint: "fixture://orders",
          }),
        ),
      );
      const runtimeContext = yield* Effect.scoped(Layer.build(adapterLayer));
      const runtimeService = Context.getUnsafe(runtimeContext, StatefulAdapter.runtimeService);
      expect(runtimeService.adapter).toBe(StatefulAdapter);
      expect(Reflect.has(runtimeService.adapter, "materializedSource")).toBe(true);
      const reporting = Option.getOrThrow(Option.fromNullishOr(runtimeService.reporting));
      expect(
        yield* reporting.dependencies({
          topic: "orders",
          lifecycle: "materialized",
          definition: { label: "orders" },
        }),
      ).toStrictEqual([{ target: "orders", endpoints: ["fixture://orders"] }]);
      expect(
        reporting.classifyFailure({ _tag: "ServerFixtureFailure", message: "down" }),
      ).toStrictEqual({ problem: "dependency", targets: ["orders"] });
      const materialized = Option.getOrThrow(Option.fromNullishOr(runtimeService.materialized));
      expect(materialized.applicationState).toBe(applicationState);
      const lifetimeScope = yield* Scope.make();
      const attemptScope = yield* Scope.make();
      expect(
        materialized.initialLaneIds?.({
          topic: "orders",
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual(["materialized", "sibling"]);
      const attempt = yield* materialized
        .acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        })
        .pipe(Effect.provideService(Scope.Scope, attemptScope));

      expect(finalized).toBe(0);
      const events = yield* attempt.lanes[0].events.pipe(Stream.take(2), Stream.runCollect);
      expect(events.map((event) => event._tag)).toStrictEqual([
        "SourceDelivery",
        "SourceItemRejection",
      ]);
      yield* Option.getOrThrow(Option.fromNullishOr(events[1])).settle(Exit.void);
      const retryTermination = yield* Effect.flip(
        materialized.retryDefault(
          Effect.fail({
            _tag: "UnexpectedCompletion",
          }),
          () => Effect.void,
        ),
      );
      expect(retryTermination).toStrictEqual({
        _tag: "UnexpectedCompletion",
      });
      yield* Scope.close(attemptScope, Exit.void);
      yield* Scope.close(lifetimeScope, Exit.void);
      expect(finalized).toBe(1);
    }),
  );

  it.effect("closes adapter service environments over attempts and settlements", () =>
    Effect.gen(function* () {
      class AdapterDependency extends Context.Service<
        AdapterDependency,
        { readonly value: string }
      >()("@effect-view-server/source-adapter/test/AdapterDependency") {}

      let applicationTransition: SourceApplicationTransition | undefined;
      let invalidShapeDefect: unknown;
      const adapterLayer = SourceAdapterServer.make(Adapter, {
        materialized: {
          acquire: (input) =>
            Effect.gen(function* () {
              const dependency = yield* AdapterDependency;
              const mutation = yield* input.toolkit.delete(dependency.value);
              const lifetimeIdentity = Object.freeze({});
              const transition = makeSourceApplicationTransition(
                input.toolkit.topic,
                () => undefined,
                [],
                lifetimeIdentity,
              );
              applicationTransition = transition;
              const invalidShapeExit = Effect.runSyncExit(
                Reflect.apply(input.toolkit.delivery, undefined, [
                  Chunk.of(mutation),
                  undefined,
                  transition,
                ]),
              );
              invalidShapeDefect = Exit.isFailure(invalidShapeExit)
                ? invalidShapeExit.cause.reasons.find(Cause.isDieReason)?.defect
                : undefined;
              const delivery = yield* input.toolkit.delivery(
                mutation,
                () => AdapterDependency.pipe(Effect.asVoid),
                transition,
              );
              const failure = yield* Adapter.failure({
                _tag: "ServerFixtureFailure",
                message: "rejected",
              }).pipe(Effect.orDie);
              const rejection = yield* input.toolkit.reject({
                failure,
                location: { offset: 2n },
                rejectedAtNanos: 3n,
                settlement: () => AdapterDependency.pipe(Effect.asVoid),
              });
              return SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "dependency",
                  events: Stream.fromEffect(
                    AdapterDependency.pipe(Effect.as([delivery, rejection])),
                  ).pipe(Stream.flatMap((events) => Stream.fromIterable(events))),
                }),
              ]);
            }),
          metrics: () =>
            AdapterDependency.pipe(
              Effect.map((dependency) => ({
                active: dependency.value.length > 0,
              })),
            ),
          retry: Schedule.recurs(0),
        },
      }).pipe(
        Layer.provide(
          Layer.succeed(AdapterDependency)({
            value: "dependency-id",
          }),
        ),
      );
      const runtimeContext = yield* Effect.scoped(Layer.build(adapterLayer));
      const runtimeService = Context.getUnsafe(runtimeContext, Adapter.runtimeService);
      const materialized = Option.getOrThrow(Option.fromNullishOr(runtimeService.materialized));
      const lifetimeScope = yield* Scope.make();
      const attempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const events = yield* attempt.lanes[0].events.pipe(Stream.take(2), Stream.runCollect);
      expect(events.map((event) => event._tag)).toStrictEqual([
        "SourceDelivery",
        "SourceItemRejection",
      ]);
      expect(Reflect.get(Option.getOrThrow(Option.fromNullishOr(events[0])), "transition")).toBe(
        applicationTransition,
      );
      expect(invalidShapeDefect).toStrictEqual(
        new TypeError("Closed Source Toolkit received an invalid transition delivery shape."),
      );
      yield* Option.getOrThrow(Option.fromNullishOr(events[0])).settle(Exit.void);
      yield* Option.getOrThrow(Option.fromNullishOr(events[1])).settle(Exit.void);
      expect(
        yield* materialized.metrics({
          topic: "orders",
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
        }),
      ).toStrictEqual({
        active: true,
      });
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it.effect("maps hostile adapter output to typed runtime failures", () =>
    Effect.gen(function* () {
      let output:
        | "forged-attempt"
        | "empty-attempt"
        | "hostile-attempt"
        | "forged-event"
        | "forged-rejection"
        | "invalid-inner-mutation"
        | "invalid-transition-batch"
        | "invalid-rejection-diagnostic"
        | "hostile-event" = "forged-attempt";
      const lifecycle: SourceAdapterServerLifecycle<
        typeof Failure.Type,
        NonNullable<typeof Adapter.materialized>,
        "materialized",
        never
      > = {
        acquire: (input) =>
          Effect.gen(function* () {
            const mutation = yield* input.toolkit.delete("hostile");
            const delivery = yield* input.toolkit.delivery(Chunk.of(mutation));
            const forgedDelivery =
              output === "hostile-event"
                ? new Proxy(delivery, {
                    get: (target, property, receiver) => {
                      if (property === "_tag") {
                        throw new Error("hostile event tag");
                      }
                      return Reflect.get(target, property, receiver);
                    },
                  })
                : new Proxy(delivery, {});
            const failure = yield* Adapter.failure({
              _tag: "ServerFixtureFailure",
              message: "type witness",
            }).pipe(Effect.orDie);
            const rejection = yield* input.toolkit.reject({
              failure,
              location: { offset: 1n },
              rejectedAtNanos: 1n,
            });
            const invalidInnerMutation = nominalClone(delivery, {
              mutations: Chunk.of(nominalClone(mutation, { id: 1 })),
            });
            const transitionDelivery = yield* input.toolkit.delivery(
              mutation,
              undefined,
              makeSourceApplicationTransition(
                input.toolkit.topic,
                () => undefined,
                [],
                Object.freeze({}),
              ),
            );
            const invalidTransitionBatch = nominalClone(transitionDelivery, {
              mutations: Chunk.make(mutation, mutation),
            });
            const invalidRejectionDiagnostic = nominalClone(rejection, {
              diagnostic: {
                ...rejection.diagnostic,
                rejectedAtNanos: "invalid",
              },
            });
            const firstEvent =
              output === "forged-rejection"
                ? new Proxy(rejection, {})
                : output === "invalid-inner-mutation"
                  ? invalidInnerMutation
                  : output === "invalid-transition-batch"
                    ? invalidTransitionBatch
                    : output === "invalid-rejection-diagnostic"
                      ? invalidRejectionDiagnostic
                      : forgedDelivery;
            const lane = SourceAdapterServer.lane({
              id: "hostile",
              events: Stream.make(firstEvent, rejection),
            });
            const validAttempt = SourceAdapterServer.attempt([lane]);
            const hostileAttempt = hostileNominalClone(validAttempt, "lanes");
            return output === "forged-attempt"
              ? new Proxy(validAttempt, {})
              : output === "empty-attempt"
                ? nominalClone(validAttempt, { lanes: [] })
                : output === "hostile-attempt"
                  ? hostileAttempt
                  : validAttempt;
          }),
        metrics: () => Effect.succeed({ active: true }),
        retry: Schedule.recurs(0),
      };
      const adapterLayer = SourceAdapterServer.make(Adapter, {
        materialized: lifecycle,
      });
      const runtimeContext = yield* Effect.scoped(Layer.build(adapterLayer));
      const runtimeService = Context.getUnsafe(runtimeContext, Adapter.runtimeService);
      const materialized = Option.getOrThrow(Option.fromNullishOr(runtimeService.materialized));
      const lifetimeScope = yield* Scope.make();

      const forgedAttemptFailure = yield* Effect.flip(
        Effect.scoped(
          materialized.acquire({
            definition: { label: "orders" },
            lifetimeScope,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ),
      );
      expect(forgedAttemptFailure).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceDefinition",
          message: "Source Adapter lifecycle returned a structurally forged Source Attempt.",
        },
      });

      output = "empty-attempt";
      const invalidNominalAttemptFailure = yield* Effect.flip(
        Effect.scoped(
          materialized.acquire({
            definition: { label: "orders" },
            lifetimeScope,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ),
      );
      expect(invalidNominalAttemptFailure).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceDefinition",
          message: "Source Adapter lifecycle returned an invalid nominal Source Attempt.",
        },
      });

      output = "hostile-attempt";
      const hostileNominalAttemptFailure = yield* Effect.flip(
        Effect.scoped(
          materialized.acquire({
            definition: { label: "orders" },
            lifetimeScope,
            target: { _tag: "Materialized" },
            toolkit,
          }),
        ),
      );
      expect(hostileNominalAttemptFailure).toStrictEqual(invalidNominalAttemptFailure);

      output = "forged-event";
      const closedAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const forgedEventFailure = yield* Effect.flip(
        closedAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
      );
      expect(forgedEventFailure).toStrictEqual({
        _tag: "RuntimeFailure",
        failure: {
          _tag: "InvalidSourceDefinition",
          message: "Source Adapter lifecycle emitted a structurally forged Source Lane Event.",
        },
      });

      output = "forged-rejection";
      const forgedRejectionAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const forgedRejectionFailure = yield* Effect.flip(
        forgedRejectionAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
      );
      expect(forgedRejectionFailure).toStrictEqual(forgedEventFailure);

      output = "invalid-inner-mutation";
      const invalidInnerMutationAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      expect(
        yield* Effect.flip(
          invalidInnerMutationAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
        ),
      ).toStrictEqual(forgedEventFailure);

      output = "invalid-transition-batch";
      const invalidTransitionBatchAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      expect(
        yield* Effect.flip(
          invalidTransitionBatchAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
        ),
      ).toStrictEqual(forgedEventFailure);

      output = "invalid-rejection-diagnostic";
      const invalidRejectionDiagnosticAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      expect(
        yield* Effect.flip(
          invalidRejectionDiagnosticAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
        ),
      ).toStrictEqual(forgedEventFailure);

      output = "hostile-event";
      const hostileEventAttempt = yield* Effect.scoped(
        materialized.acquire({
          definition: { label: "orders" },
          lifetimeScope,
          target: { _tag: "Materialized" },
          toolkit,
        }),
      );
      const hostileEventFailure = yield* Effect.flip(
        hostileEventAttempt.lanes[0].events.pipe(Stream.take(1), Stream.runDrain),
      );
      expect(hostileEventFailure).toStrictEqual(forgedEventFailure);
      yield* Scope.close(lifetimeScope, Exit.void);
    }),
  );

  it("rejects missing, extra, or structurally copied runtime linkage", () => {
    const dualAdapter = SourceAdapter.make({
      identity: { name: "dual-server-fixture" },
      failure: Failure,
      materialized: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
      leased: {
        metrics: Metrics,
        rejectionLocation: Location,
        definitionOptions: SourceAdapter.definitionOptions<void>(),
      },
    });
    const lifecycle = {
      acquire: () =>
        Effect.succeed(
          SourceAdapterServer.attempt([
            SourceAdapterServer.lane({
              id: "fixture",
              events: Stream.never,
            }),
          ]),
        ),
      metrics: () => Effect.succeed({ active: true }),
      retry: Schedule.recurs(0),
    };
    const copiedAdapter = Object.defineProperties({}, Object.getOwnPropertyDescriptors(Adapter));

    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [
        dualAdapter,
        { materialized: lifecycle },
      ]),
    ).toThrow("implement exactly");
    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [dualAdapter, { leased: lifecycle }]),
    ).toThrow("implement exactly");
    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [
        Adapter,
        { materialized: lifecycle, leased: lifecycle },
      ]),
    ).toThrow("implement exactly");
    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [
        Adapter,
        {
          materialized: {
            ...lifecycle,
            applicationState: {},
          },
        },
      ]),
    ).toThrow("application state registration must exactly match");
    expect(() =>
      Reflect.apply(SourceAdapterServer.make, undefined, [
        copiedAdapter,
        { materialized: lifecycle },
      ]),
    ).toThrow("nominal Source Adapter descriptor");
  });

  it.effect("builds leased-only services and rejects undefined implementations", () =>
    Effect.gen(function* () {
      const leasedAdapter = SourceAdapter.make({
        identity: { name: "leased-only-server-fixture" },
        failure: Failure,
        materialized: undefined,
        leased: {
          metrics: Metrics,
          rejectionLocation: Location,
          definitionOptions: SourceAdapter.definitionOptions<void>(),
        },
      });
      const leasedLayer = SourceAdapterServer.make(leasedAdapter, {
        leased: {
          acquire: () =>
            Effect.succeed(
              SourceAdapterServer.attempt([
                SourceAdapterServer.lane({
                  id: "leased",
                  events: Stream.never,
                }),
              ]),
            ),
          metrics: () => Effect.succeed({ active: true }),
          retry: Schedule.recurs(0),
        },
      });
      const leasedContext = yield* Effect.scoped(Layer.build(leasedLayer));
      const leasedService = Context.getUnsafe(leasedContext, leasedAdapter.runtimeService);
      expect(leasedService.materialized).toBeUndefined();
      expect(leasedService.leased).toBeDefined();

      const invalidMaterializedLayer: Layer.Layer<never> = Reflect.apply(
        SourceAdapterServer.make,
        undefined,
        [Adapter, { materialized: undefined }],
      );
      const invalidLeasedLayer: Layer.Layer<never> = Reflect.apply(
        SourceAdapterServer.make,
        undefined,
        [leasedAdapter, { leased: undefined }],
      );
      const invalidMaterialized = yield* Effect.exit(
        Effect.scoped(Layer.build(invalidMaterializedLayer)),
      );
      const invalidLeased = yield* Effect.exit(Effect.scoped(Layer.build(invalidLeasedLayer)));
      expect(Exit.isFailure(invalidMaterialized)).toBe(true);
      expect(Exit.isFailure(invalidLeased)).toBe(true);
    }),
  );

  it.effect("validates lane IDs and supplies unbuffered metrics by default", () =>
    Effect.gen(function* () {
      expect(() =>
        SourceAdapterServer.lane({
          id: "",
          events: Stream.never,
        }),
      ).toThrow("must be non-empty");

      const lane = SourceAdapterServer.lane({
        id: "fixture",
        events: Stream.never,
      });
      expect(yield* lane.bufferMetrics).toStrictEqual({
        _tag: "Unbuffered",
      });
      const fiber = yield* lane.events.pipe(Stream.runDrain, Effect.forkChild);
      yield* Fiber.interrupt(fiber);
    }),
  );
});
