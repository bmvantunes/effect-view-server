import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Exit,
  HashMap,
  HashSet,
  Option,
  Result,
  Scope,
} from "effect";
import type {
  SourceApplicationStateModule,
  SourceApplicationStateSweepInput,
} from "effect-view-server/source-adapter/server";
import {
  SourceAdapterServer,
  epochNanosFromWallMillis,
} from "effect-view-server/source-adapter/server";
import {
  kafkaBrokerContractKey,
  type KafkaResolvedBrokerContract,
  resolveKafkaRetention,
  snapshotKafkaResolvedBrokerContract,
} from "./broker-contract";
import {
  KafkaSourceConfigurationError,
  type KafkaAdapterFailure,
  type KafkaCapturedStartPosition,
  type KafkaExpirationFailure,
  type KafkaResolvedStartPosition,
  type KafkaRetentionMetrics,
  type KafkaRuntimeDefinitionOptions,
} from "./contract";
import {
  KafkaRetentionDeadlineIndex,
  type KafkaRetentionDeadline,
} from "./retention-deadline-index";

export { KafkaRetentionDeadlineIndex } from "./retention-deadline-index";
export type { KafkaRetentionDeadline } from "./retention-deadline-index";

export type KafkaRuntimeDefinition = KafkaRuntimeDefinitionOptions;

export const configurationFailure = <Region extends string = string>(
  message: string,
): KafkaAdapterFailure<Region> => ({
  _tag: "KafkaConfigurationFailure",
  message,
});

export { epochNanosFromWallMillis };

export const currentEpochNanos = Effect.fn("KafkaSourceAdapter.clock.epochNanos")(function* () {
  const millis = yield* Clock.currentTimeMillis;
  return yield* Effect.try({
    try: () => epochNanosFromWallMillis(millis),
    catch: () =>
      configurationFailure("Effect Clock returned an invalid wall-clock millisecond value."),
  });
});

const nanosToKafkaMillis = (nanos: bigint): bigint =>
  nanos === 0n ? 0n : (nanos + 999_999n) / 1_000_000n;

export const resolveStart = Effect.fn("KafkaSourceAdapter.start.resolve")(function* (
  start: KafkaCapturedStartPosition,
): Effect.fn.Return<KafkaResolvedStartPosition, KafkaAdapterFailure> {
  if (start === "earliest") {
    return {
      mode: "earliest",
    };
  }
  if (start === "latest") {
    return {
      mode: "latest",
    };
  }
  if (start.mode === "committed") {
    return {
      mode: start.mode,
      consumerGroupId: start.consumerGroupId,
      fallback: start.fallback,
    };
  }
  if (start.mode === "timestamp") {
    return {
      mode: start.mode,
      atNanos: start.atNanos,
      atMillis: nanosToKafkaMillis(start.atNanos),
      fallback: start.fallback,
    };
  }
  const resolvedAtNanos = yield* currentEpochNanos();
  const atNanos =
    resolvedAtNanos > start.durationNanos ? resolvedAtNanos - start.durationNanos : 0n;
  return {
    mode: start.mode,
    durationNanos: start.durationNanos,
    resolvedAtNanos,
    atNanos,
    atMillis: nanosToKafkaMillis(atNanos),
    fallback: start.fallback,
  };
});

export type KafkaKeyLease = {
  readonly release: Effect.Effect<void>;
  readonly releaseNow: () => void;
};

export type KafkaKeyLeaseRegistry = {
  readonly acquire: (id: string) => Effect.Effect<KafkaKeyLease>;
  readonly users: (id: string) => number;
};

type KafkaKeyLeaseEntry = {
  held: boolean;
  head: KafkaKeyLeaseWaiter | undefined;
  tail: KafkaKeyLeaseWaiter | undefined;
  users: number;
};

type KafkaKeyLeaseWaiter = {
  readonly deferred: Deferred.Deferred<void>;
  previous: KafkaKeyLeaseWaiter | undefined;
  next: KafkaKeyLeaseWaiter | undefined;
  queued: boolean;
};

const removeKafkaKeyLeaseWaiter = (
  entry: KafkaKeyLeaseEntry,
  waiter: KafkaKeyLeaseWaiter,
): void => {
  waiter.queued = false;
  if (waiter.previous === undefined) {
    entry.head = waiter.next;
  } else {
    waiter.previous.next = waiter.next;
  }
  if (waiter.next === undefined) {
    entry.tail = waiter.previous;
  } else {
    waiter.next.previous = waiter.previous;
  }
  waiter.previous = undefined;
  waiter.next = undefined;
};

const appendKafkaKeyLeaseWaiter = (
  entry: KafkaKeyLeaseEntry,
  waiter: KafkaKeyLeaseWaiter,
): void => {
  waiter.previous = entry.tail;
  if (entry.tail === undefined) {
    entry.head = waiter;
  } else {
    entry.tail.next = waiter;
  }
  entry.tail = waiter;
  waiter.queued = true;
};

const releaseKafkaKeyLeaseOwnership = (
  entries: Map<string, KafkaKeyLeaseEntry>,
  id: string,
  entry: KafkaKeyLeaseEntry,
  afterOwnershipTransferred: () => void,
): void => {
  entry.users -= 1;
  const next = entry.head;
  if (next === undefined) {
    entry.held = false;
  } else {
    removeKafkaKeyLeaseWaiter(entry, next);
    afterOwnershipTransferred();
    Deferred.doneUnsafe(next.deferred, Effect.void);
  }
  if (!entry.held && entry.users === 0 && entries.get(id) === entry) {
    entries.delete(id);
  }
};

export const makeKafkaKeyLeaseRegistry = (
  afterWaiterGranted: Effect.Effect<void> = Effect.void,
  afterOwnershipTransferred: () => void = () => undefined,
): KafkaKeyLeaseRegistry => {
  const entries = new Map<string, KafkaKeyLeaseEntry>();
  const acquire: KafkaKeyLeaseRegistry["acquire"] = (id) =>
    Effect.uninterruptibleMask(() =>
      Effect.gen(function* () {
        const entry = entries.get(id) ?? {
          held: false,
          head: undefined,
          tail: undefined,
          users: 0,
        };
        entries.set(id, entry);
        entry.users += 1;
        if (entry.held) {
          const waiter: KafkaKeyLeaseWaiter = {
            deferred: Deferred.makeUnsafe(),
            previous: undefined,
            next: undefined,
            queued: false,
          };
          appendKafkaKeyLeaseWaiter(entry, waiter);
          const awaited = yield* Effect.exit(Effect.interruptible(Deferred.await(waiter.deferred)));
          if (Exit.isFailure(awaited)) {
            if (waiter.queued) {
              removeKafkaKeyLeaseWaiter(entry, waiter);
              entry.users -= 1;
            } else {
              releaseKafkaKeyLeaseOwnership(entries, id, entry, afterOwnershipTransferred);
            }
            return yield* Effect.failCause(awaited.cause);
          }
          const granted = yield* Effect.exit(Effect.interruptible(afterWaiterGranted));
          if (Exit.isFailure(granted)) {
            releaseKafkaKeyLeaseOwnership(entries, id, entry, afterOwnershipTransferred);
            return yield* Effect.failCause(granted.cause);
          }
        } else {
          entry.held = true;
        }
        let released = false;
        let lease: KafkaKeyLease;
        const releaseNow = () => {
          if (released) {
            return;
          }
          released = true;
          releaseKafkaKeyLeaseOwnership(entries, id, entry, afterOwnershipTransferred);
        };
        lease = {
          release: Effect.sync(releaseNow),
          releaseNow,
        };
        return lease;
      }),
    );
  return {
    acquire,
    users: (id) => entries.get(id)?.users ?? 0,
  };
};

export const completeKafkaDelivery = <Value, Error>(
  delivery: Exit.Exit<Value, Error>,
  lease: {
    readonly release: Effect.Effect<void>;
  },
): Effect.Effect<Value, Error> =>
  Effect.gen(function* () {
    if (Exit.isFailure(delivery)) {
      yield* lease.release;
      return yield* Effect.failCause(delivery.cause);
    }
    return delivery.value;
  });

class PersistentHashMap<Key, Value> implements Iterable<readonly [Key, Value]> {
  readonly #values: HashMap.HashMap<Key, Value>;

  constructor(values: HashMap.HashMap<Key, Value> = HashMap.empty()) {
    this.#values = values;
    Object.freeze(this);
  }

  get size(): number {
    return HashMap.size(this.#values);
  }

  get(key: Key): Value | undefined {
    return Option.getOrUndefined(HashMap.get(this.#values, key));
  }

  set(key: Key, value: Value): PersistentHashMap<Key, Value> {
    return new PersistentHashMap(HashMap.set(this.#values, key, value));
  }

  remove(key: Key): PersistentHashMap<Key, Value> {
    return new PersistentHashMap(HashMap.remove(this.#values, key));
  }

  entries(): IterableIterator<[Key, Value]> {
    return HashMap.entries(this.#values);
  }

  keys(): IterableIterator<Key> {
    return HashMap.keys(this.#values);
  }

  [Symbol.iterator](): IterableIterator<[Key, Value]> {
    return this.entries();
  }
}

class PersistentHashSet<Value> {
  readonly #values: HashSet.HashSet<Value>;

  constructor(values: HashSet.HashSet<Value> = HashSet.empty()) {
    this.#values = values;
    Object.freeze(this);
  }

  get size(): number {
    return HashSet.size(this.#values);
  }

  add(value: Value): PersistentHashSet<Value> {
    return new PersistentHashSet(HashSet.add(this.#values, value));
  }

  remove(value: Value): PersistentHashSet<Value> {
    return new PersistentHashSet(HashSet.remove(this.#values, value));
  }
}

type KafkaRetentionRegionState = {
  readonly contract: KafkaResolvedBrokerContract;
  readonly failedWork: PersistentHashSet<string>;
  readonly trackedRows: number;
  readonly expiredRows: bigint;
  readonly authoritativeExpiredDeletes: bigint;
  readonly expirationRetryFailures: bigint;
  readonly latestExpirationFailure: KafkaExpirationFailure | null;
  readonly lastSweepAtNanos: bigint | null;
  readonly lastSweepDurationNanos: bigint | null;
  readonly lastSweepRetryableFailures: number;
};

export type KafkaRetentionState = {
  readonly topic: string;
  readonly deadlines: PersistentHashMap<string, KafkaRetentionDeadline>;
  readonly deadlineIndex: KafkaRetentionDeadlineIndex;
  readonly regions: PersistentHashMap<string, KafkaRetentionRegionState>;
  readonly nextGeneration: bigint;
};

type KafkaRetentionRuntime = {
  readonly keyLeases: KafkaKeyLeaseRegistry;
  nextGeneration: bigint;
};

const retentionRuntimes = new WeakMap<KafkaRetentionState, KafkaRetentionRuntime>();

const snapshotKafkaRetentionState = (
  input: KafkaRetentionState,
  runtime: KafkaRetentionRuntime,
): KafkaRetentionState => {
  const state = Object.freeze(input);
  retentionRuntimes.set(state, runtime);
  return state;
};

export const retentionDeadlineOrder = (
  state: KafkaRetentionState,
): ReadonlyArray<KafkaRetentionDeadline> => [...state.deadlineIndex];

export type KafkaRetentionCommand =
  | {
      readonly _tag: "AppliedUpsert";
      readonly id: string;
      readonly region: string;
      readonly deadlineNanos: bigint | null;
    }
  | {
      readonly _tag: "AppliedDelete";
      readonly id: string;
      readonly region: string;
      readonly authoritativeExpired: boolean;
    }
  | {
      readonly _tag: "ExpirationSucceeded";
      readonly candidate: KafkaRetentionDeadline;
      readonly workId: string;
    }
  | {
      readonly _tag: "ExpirationFailed";
      readonly candidate: KafkaRetentionDeadline;
      readonly workId: string;
      readonly failure: KafkaExpirationFailure;
    }
  | {
      readonly _tag: "ExpirationStale";
      readonly candidate: KafkaRetentionDeadline;
      readonly workId: string;
    }
  | {
      readonly _tag: "SweepCompleted";
      readonly completedAtNanos: bigint;
      readonly durationNanos: bigint;
      readonly retryableFailuresByRegion: ReadonlyMap<string, number>;
    };

export const retentionWorkId = (deadline: KafkaRetentionDeadline): string =>
  `${deadline.id}\u0000${deadline.generation}`;

export const retentionWorkForId = (
  state: KafkaRetentionState,
  id: string,
): ReadonlyArray<string> => {
  const existing = state.deadlines.get(id);
  return existing === undefined ? [] : [retentionWorkId(existing)];
};

export const reduceKafkaRetentionState = (
  state: KafkaRetentionState,
  command: KafkaRetentionCommand,
): KafkaRetentionState => {
  const runtime = retentionRuntimes.get(state);
  if (runtime === undefined) {
    throw new KafkaSourceConfigurationError(
      "Kafka retention state lost its lifetime generation sequence.",
    );
  }
  let deadlines = state.deadlines;
  let deadlineIndex = state.deadlineIndex;
  let regions = state.regions;
  const updateRegion = (
    region: string,
    update: (current: KafkaRetentionRegionState) => KafkaRetentionRegionState,
  ): void => {
    const current = regions.get(region);
    if (current !== undefined) {
      regions = regions.set(region, Object.freeze(update(current)));
    }
  };
  const removeCurrent = (id: string): KafkaRetentionDeadline | undefined => {
    const existing = deadlines.get(id);
    if (existing === undefined) {
      return undefined;
    }
    deadlines = deadlines.remove(id);
    deadlineIndex = deadlineIndex.remove(existing);
    updateRegion(existing.region, (current) => ({
      ...current,
      trackedRows: current.trackedRows - 1,
    }));
    return existing;
  };
  const nextState = (nextGeneration = runtime.nextGeneration): KafkaRetentionState => {
    runtime.nextGeneration = nextGeneration;
    return snapshotKafkaRetentionState(
      {
        topic: state.topic,
        deadlines,
        deadlineIndex,
        regions,
        nextGeneration,
      },
      runtime,
    );
  };
  if (command._tag === "AppliedUpsert") {
    const existing = deadlines.get(command.id);
    if (existing !== undefined) {
      updateRegion(existing.region, (current) => ({
        ...current,
        failedWork: current.failedWork.remove(retentionWorkId(existing)),
      }));
    }
    removeCurrent(command.id);
    if (command.deadlineNanos === null) {
      return nextState();
    }
    const nextGeneration = runtime.nextGeneration + 1n;
    const deadline = Object.freeze({
      id: command.id,
      region: command.region,
      deadlineNanos: command.deadlineNanos,
      generation: nextGeneration,
    });
    deadlines = deadlines.set(command.id, deadline);
    deadlineIndex = deadlineIndex.set(deadline);
    updateRegion(command.region, (current) => ({
      ...current,
      trackedRows: current.trackedRows + 1,
    }));
    return nextState(nextGeneration);
  }
  if (command._tag === "AppliedDelete") {
    const existing = deadlines.get(command.id);
    if (existing !== undefined) {
      updateRegion(existing.region, (current) => ({
        ...current,
        failedWork: current.failedWork.remove(retentionWorkId(existing)),
      }));
    }
    removeCurrent(command.id);
    if (command.authoritativeExpired) {
      updateRegion(command.region, (current) => ({
        ...current,
        authoritativeExpiredDeletes: current.authoritativeExpiredDeletes + 1n,
      }));
    }
    return nextState();
  }
  if (command._tag === "SweepCompleted") {
    for (const [region, regionState] of regions) {
      regions = regions.set(
        region,
        Object.freeze({
          ...regionState,
          lastSweepAtNanos: command.completedAtNanos,
          lastSweepDurationNanos: command.durationNanos,
          lastSweepRetryableFailures: command.retryableFailuresByRegion.get(region) ?? 0,
        }),
      );
    }
    return nextState();
  }
  if (command._tag === "ExpirationFailed") {
    updateRegion(command.candidate.region, (current) => ({
      ...current,
      failedWork: current.failedWork.add(command.workId),
      expirationRetryFailures: current.expirationRetryFailures + 1n,
      latestExpirationFailure: command.failure,
    }));
    return nextState();
  }
  updateRegion(command.candidate.region, (current) => ({
    ...current,
    failedWork: current.failedWork.remove(command.workId),
  }));
  if (command._tag === "ExpirationSucceeded") {
    const current = deadlines.get(command.candidate.id);
    if (current?.generation === command.candidate.generation) {
      removeCurrent(command.candidate.id);
      updateRegion(command.candidate.region, (region) => ({
        ...region,
        expiredRows: region.expiredRows + 1n,
      }));
    }
  }
  return nextState();
};

export const retentionMetrics = (
  state: KafkaRetentionState,
  region: string,
  sweepIntervalNanos: bigint,
): KafkaRetentionMetrics => {
  const regionState = Option.getOrThrow(Option.fromUndefinedOr(state.regions.get(region)));
  return {
    declaredCleanupPolicy: regionState.contract.cleanupPolicy,
    observedCleanupPolicy: regionState.contract.observedCleanupPolicy,
    configuredRetention: regionState.contract.retentionPolicy,
    resolvedRetention: regionState.contract.resolvedRetention,
    trackedRows: regionState.trackedRows,
    lastSweepRetryableFailures: regionState.lastSweepRetryableFailures,
    expiredRows: regionState.expiredRows,
    authoritativeExpiredDeletes: regionState.authoritativeExpiredDeletes,
    failedWorkBacklog: regionState.failedWork.size,
    expirationRetryFailures: regionState.expirationRetryFailures,
    latestExpirationFailure: regionState.latestExpirationFailure,
    lastSweepAtNanos: regionState.lastSweepAtNanos,
    lastSweepDurationNanos: regionState.lastSweepDurationNanos,
    sweepIntervalNanos,
  };
};

export type KafkaSweepOutcome = {
  readonly retryableFailuresByRegion: ReadonlyMap<string, number>;
};

export const isKafkaRuntimeDefinition = (value: unknown): value is KafkaRuntimeDefinition =>
  Result.try(() => {
    if (typeof value !== "object" || value === null) {
      return false;
    }
    const cleanupPolicy = Reflect.get(value, "cleanupPolicy");
    const topic = Reflect.get(value, "topic");
    const regions = Reflect.get(value, "regions");
    const retentionPolicy = Reflect.get(value, "retentionPolicy");
    if (
      (cleanupPolicy !== "delete" &&
        cleanupPolicy !== "compact" &&
        cleanupPolicy !== "compact-and-delete") ||
      typeof topic !== "string" ||
      topic.length === 0 ||
      !Array.isArray(regions) ||
      regions.length === 0 ||
      regions.some((region) => typeof region !== "string" || region.length === 0) ||
      typeof retentionPolicy !== "object" ||
      retentionPolicy === null
    ) {
      return false;
    }
    const retentionTag = Reflect.get(retentionPolicy, "_tag");
    return (
      retentionTag === "MatchKafkaRetention" ||
      retentionTag === "Forever" ||
      (retentionTag === "Finite" &&
        typeof Reflect.get(retentionPolicy, "durationNanos") === "bigint" &&
        Reflect.get(retentionPolicy, "durationNanos") > 0n)
    );
  }).pipe(
    Result.match({
      onFailure: () => false,
      onSuccess: (valid) => valid,
    }),
  );

export const makeKafkaRetentionState = (
  topic: string,
  contracts: ReadonlyArray<KafkaResolvedBrokerContract>,
): KafkaRetentionState =>
  snapshotKafkaRetentionState(
    {
      topic,
      deadlines: new PersistentHashMap(),
      deadlineIndex: new KafkaRetentionDeadlineIndex(),
      regions: contracts.reduce((regions, contract) => {
        const snapshot = snapshotKafkaResolvedBrokerContract(contract);
        return regions.set(
          snapshot.region,
          Object.freeze({
            contract: snapshot,
            failedWork: new PersistentHashSet<string>(),
            trackedRows: 0,
            expiredRows: 0n,
            authoritativeExpiredDeletes: 0n,
            expirationRetryFailures: 0n,
            latestExpirationFailure: null,
            lastSweepAtNanos: null,
            lastSweepDurationNanos: null,
            lastSweepRetryableFailures: 0,
          }),
        );
      }, new PersistentHashMap<string, KafkaRetentionRegionState>()),
      nextGeneration: 0n,
    },
    {
      keyLeases: makeKafkaKeyLeaseRegistry(),
      nextGeneration: 0n,
    },
  );

export const acquireKafkaRetentionKeyLease = (
  state: KafkaRetentionState,
  id: string,
): Effect.Effect<KafkaKeyLease, never, Scope.Scope> => {
  const runtime = retentionRuntimes.get(state);
  if (runtime === undefined) {
    return Effect.die(
      new KafkaSourceConfigurationError(
        "Kafka retention state lost its lifetime generation sequence.",
      ),
    );
  }
  return runtime.keyLeases.acquire(id);
};

export const kafkaRetentionKeyLeaseUsers = (state: KafkaRetentionState, id: string): number =>
  retentionRuntimes.get(state)?.keyLeases.users(id) ?? 0;

export const acquireKafkaTransitionLease = Effect.fn(
  "KafkaSourceAdapter.keyedLease.acquireTransition",
)(function* (state: KafkaRetentionState, command: KafkaRetentionCommand) {
  const id =
    command._tag === "AppliedUpsert" || command._tag === "AppliedDelete" ? command.id : undefined;
  if (id === undefined) {
    return yield* Effect.die(
      new KafkaSourceConfigurationError(
        "Kafka delivery transition requires an applied Upsert or Delete command.",
      ),
    );
  }
  const runtime = retentionRuntimes.get(state);
  if (runtime === undefined) {
    return yield* Effect.die(
      new KafkaSourceConfigurationError(
        "Kafka retention state lost its lifetime generation sequence.",
      ),
    );
  }
  return yield* runtime.keyLeases.acquire(id);
});

export const runKafkaDueSweep = Effect.fn("KafkaSourceAdapter.retention.runDueSweep")(function* <
  Topic extends string,
>(
  input: SourceApplicationStateSweepInput<Topic, KafkaRetentionState, KafkaRetentionCommand>,
): Effect.fn.Return<KafkaSweepOutcome> {
  const sweepStartedMonotonicNanos = yield* Clock.monotonicTimeNanos;
  const runtime = retentionRuntimes.get(input.state);
  if (runtime === undefined) {
    return yield* Effect.die(
      new KafkaSourceConfigurationError(
        "Kafka retention state lost its lifetime generation sequence.",
      ),
    );
  }
  const retryableFailuresByRegion = new Map<string, number>();
  const remaining = input.state.deadlineIndex.values();
  return yield* Effect.gen(function* () {
    let next = remaining.next();
    while (true) {
      const batch: Array<KafkaRetentionDeadline> = [];
      while (batch.length < 256) {
        if (next.done || next.value.deadlineNanos > input.epochNowNanos) {
          break;
        }
        const candidate = next.value;
        next = remaining.next();
        batch.push(candidate);
      }
      if (batch.length === 0) {
        break;
      }
      for (const candidate of batch) {
        const workId = retentionWorkId(candidate);
        const result = yield* Effect.acquireUseRelease(
          Effect.interruptible(runtime.keyLeases.acquire(candidate.id)),
          () =>
            input.execute(
              input.operation({
                id: candidate.id,
                workId,
                isCurrent: (state) =>
                  state.deadlines.get(candidate.id)?.generation === candidate.generation,
                onSuccess: {
                  _tag: "ExpirationSucceeded",
                  candidate,
                  workId,
                },
                onFailure: () => ({
                  _tag: "ExpirationFailed",
                  candidate,
                  workId,
                  failure: {
                    region: candidate.region,
                    topic: input.state.topic,
                    id: candidate.id,
                    generation: candidate.generation,
                    failedAtNanos: input.epochNowNanos,
                    message: "Kafka retention expiration Delete failed.",
                  },
                }),
                onStale: {
                  _tag: "ExpirationStale",
                  candidate,
                  workId,
                },
              }),
            ),
          (lease) => lease.release,
        );
        if (result._tag === "Inactive") {
          return {
            retryableFailuresByRegion,
          };
        }
        if (
          result._tag === "Applied" &&
          Exit.isFailure(result.exit) &&
          !Cause.hasInterruptsOnly(result.exit.cause)
        ) {
          retryableFailuresByRegion.set(
            candidate.region,
            (retryableFailuresByRegion.get(candidate.region) ?? 0) + 1,
          );
        }
      }
      yield* Effect.yieldNow;
    }
    const sweepFinishedMonotonicNanos = yield* Clock.monotonicTimeNanos;
    const sweepFinishedEpochNanos = yield* currentEpochNanos().pipe(Effect.orDie);
    input.update({
      _tag: "SweepCompleted",
      completedAtNanos: sweepFinishedEpochNanos,
      // Duration is elapsed monotonic time; completedAtNanos is the distinct
      // public epoch-wall timestamp and must never be arithmetically combined with it.
      durationNanos: sweepFinishedMonotonicNanos - sweepStartedMonotonicNanos,
      retryableFailuresByRegion,
    });
    return {
      retryableFailuresByRegion,
    };
  });
});

export const makeKafkaApplicationStateRegistration = (
  resolveContracts: (
    topic: string,
    definition: KafkaRuntimeDefinition,
  ) => ReadonlyArray<KafkaResolvedBrokerContract>,
  sweepIntervalNanos: bigint,
) =>
  SourceAdapterServer.applicationState({
    sweepIntervalNanos,
    initialState: (binding) => {
      if (!isKafkaRuntimeDefinition(binding.definition)) {
        throw new KafkaSourceConfigurationError(
          "Kafka Application State received an invalid Source Definition.",
        );
      }
      const contracts = resolveContracts(binding.topic, binding.definition);
      return makeKafkaRetentionState(binding.topic, contracts);
    },
    reduce: reduceKafkaRetentionState,
    cancelledMaintenanceWorkIds: (state, command) =>
      command._tag === "AppliedUpsert" || command._tag === "AppliedDelete"
        ? retentionWorkForId(state, command.id)
        : [],
    acquireTransition: (state, command) =>
      acquireKafkaTransitionLease(state, command).pipe(Effect.map((lease) => lease.releaseNow)),
    metrics: (state) =>
      new Map(
        [...state.regions.keys()].map((region) => [
          region,
          retentionMetrics(state, region, sweepIntervalNanos),
        ]),
      ),
    runDueSweep: runKafkaDueSweep,
  });

export type KafkaApplicationState<Topic extends string = string> = SourceApplicationStateModule<
  Topic,
  KafkaRetentionState,
  KafkaRetentionCommand,
  ReadonlyMap<string, KafkaRetentionMetrics>,
  KafkaSweepOutcome
>;

export const resolveKafkaContracts = (
  viewServerTopic: string,
  definition: KafkaRuntimeDefinition,
  brokerContracts: ReadonlyMap<string, KafkaResolvedBrokerContract>,
): ReadonlyArray<KafkaResolvedBrokerContract> => {
  const resolved: Array<KafkaResolvedBrokerContract> = [];
  const failures: Array<string> = [];
  for (const region of definition.regions) {
    const configured = brokerContracts.get(kafkaBrokerContractKey(viewServerTopic, region));
    if (configured === undefined) {
      failures.push(
        `Kafka broker contract for Topic ${viewServerTopic} Region ${region} is unavailable.`,
      );
      continue;
    }
    const declaredRetentionMatches =
      configured.retentionPolicy._tag === definition.retentionPolicy._tag &&
      (configured.retentionPolicy._tag !== "Finite" ||
        (definition.retentionPolicy._tag === "Finite" &&
          configured.retentionPolicy.durationNanos === definition.retentionPolicy.durationNanos));
    const expectedResolvedRetention = resolveKafkaRetention(
      definition.cleanupPolicy,
      definition.retentionPolicy,
      configured.observedRetentionMs,
    );
    const resolvedRetentionMatches =
      configured.resolvedRetention._tag === expectedResolvedRetention._tag &&
      (configured.resolvedRetention._tag !== "Finite" ||
        (expectedResolvedRetention._tag === "Finite" &&
          configured.resolvedRetention.durationNanos === expectedResolvedRetention.durationNanos));
    if (
      configured.viewServerTopic !== viewServerTopic ||
      configured.sourceTopic !== definition.topic ||
      configured.region !== region ||
      configured.cleanupPolicy !== definition.cleanupPolicy ||
      configured.observedCleanupPolicy !== definition.cleanupPolicy ||
      !declaredRetentionMatches ||
      !resolvedRetentionMatches
    ) {
      failures.push(
        `Kafka broker contract for Topic ${viewServerTopic} Region ${region} does not match its Source Definition.`,
      );
      continue;
    }
    resolved.push(configured);
  }
  if (failures.length > 0) {
    throw new KafkaSourceConfigurationError(failures.join(" "));
  }
  return resolved;
};
