import type {
  ExactPatch,
  LiveQueryRow,
  LiveQueryResult,
  TopicRow,
} from "@effect-view-server/config";
import { viewServerUnsupportedRuntimeFieldDomain } from "@effect-view-server/config";
import {
  snapshotViewServerQuery,
  viewServerQuerySnapshotErrorMessage,
} from "@effect-view-server/effect-utils";
import {
  snapshotViewServerTopics,
  viewServerRowSchemaFieldsMatchAst,
} from "@effect-view-server/config/internal";
import { Effect, Latch, Result, Schema, Semaphore } from "effect";
import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  ColumnLiveViewEngineInternalConfig,
  ColumnLiveViewEngineInternal,
  ColumnLiveViewSubscription,
  ColumnLiveViewTerminalObserver,
  DecodableTopicDefinitions,
  ExactEngineLiveQueryInputForTopic,
} from "./engine-contract";
import {
  EngineClosedError,
  InvalidRowError,
  InvalidTopicError,
  type ColumnLiveViewEngineError,
} from "./engine-errors";
import { collectColumnLiveViewEngineHealth } from "./engine-health";
import {
  groupedIncrementalAdmissionLimitsFromConfig,
  type GroupedIncrementalAdmissionLimits,
} from "./grouped-incremental-admission";
import type { LiveSubscription } from "./live-subscription";
import { snapshotRuntimeExecutableQuery, subscribeRuntimeExecutableQuery } from "./query-execution";
import type { ColumnLiveViewEngineQueryPartition } from "./query-partition";
import { InvalidQueryError } from "./raw-query-compiler";
import {
  acquireTopicStoreSubscription,
  closeTopicStoreSubscriptions,
  deleteTopicStoreRow,
  patchTopicStoreDecodedFields,
  patchTopicStoreRow,
  publishTopicStoreDecodedRows,
  publishTopicStoreDecodedRowsWithStorageKeys,
  publishTopicStoreRow,
  publishTopicStoreRows,
  publishTopicStoreRowsWithStorageKeys,
  resetTopicStore,
  TopicStore,
} from "./topic-store";

const defaultSubscriptionQueueCapacity = 1_024;

const unobservedTerminal: ColumnLiveViewTerminalObserver = {
  onQueryRegistered: () => Effect.void,
  onTerminalOccurrence: () => Effect.void,
  onTerminalReady: () => Effect.void,
};

const invalidRow = (topic: string, message: string) =>
  new InvalidRowError({
    topic,
    message,
  });

type EngineTopicsInspection<Topics extends DecodableTopicDefinitions> =
  | {
      readonly _tag: "Invalid";
      readonly error: InvalidRowError;
    }
  | {
      readonly _tag: "Valid";
      readonly topics: Topics;
    };

const inspectEngineTopics = <Topics extends DecodableTopicDefinitions>(
  topics: Topics,
): EngineTopicsInspection<Topics> => {
  const snapshot = snapshotViewServerTopics(topics);
  for (const [topic, definition] of Object.entries(snapshot)) {
    const schema = definition.schema;
    if (!Schema.isSchema(schema) || !("fields" in schema)) {
      return {
        _tag: "Invalid",
        error: invalidRow(topic, "Topic row schema must be an Effect Schema Struct."),
      };
    }
    for (const [field, fieldSchema] of Object.entries(schema.fields)) {
      if (!Schema.isSchema(fieldSchema)) {
        return {
          _tag: "Invalid",
          error: invalidRow(topic, `Topic field ${field} must be an Effect Schema.`),
        };
      }
      const unsupportedRuntimeDomain = viewServerUnsupportedRuntimeFieldDomain(fieldSchema);
      if (unsupportedRuntimeDomain !== undefined) {
        return {
          _tag: "Invalid",
          error: invalidRow(
            topic,
            `Topic field ${field} uses unsupported runtime domain: ${unsupportedRuntimeDomain}`,
          ),
        };
      }
    }
    const unsupportedRowRuntimeDomain = viewServerUnsupportedRuntimeFieldDomain(schema);
    if (unsupportedRowRuntimeDomain !== undefined) {
      return {
        _tag: "Invalid",
        error: invalidRow(
          topic,
          `Topic row schema uses unsupported runtime domain: ${unsupportedRowRuntimeDomain}`,
        ),
      };
    }
    if (!viewServerRowSchemaFieldsMatchAst(schema)) {
      return {
        _tag: "Invalid",
        error: invalidRow(topic, "Topic exposed row fields do not match the row schema AST."),
      };
    }
  }
  return {
    _tag: "Valid",
    topics: snapshot,
  };
};

const snapshotAndValidateEngineTopics = Effect.fn("ColumnLiveViewEngine.topics.snapshot")(
  <Topics extends DecodableTopicDefinitions>(
    topics: Topics,
  ): Effect.Effect<Topics, InvalidRowError> =>
    Effect.try({
      try: () => inspectEngineTopics(topics),
      catch: () =>
        invalidRow(
          "<engine-config>",
          "Topic schemas could not be safely inspected during engine construction.",
        ),
    }).pipe(
      Effect.flatMap((inspection) =>
        inspection._tag === "Invalid"
          ? Effect.fail(inspection.error)
          : Effect.succeed(inspection.topics),
      ),
    ),
);

const runEngineLifecycleTransaction = Effect.fn("ColumnLiveViewEngine.lifecycle.transaction")(
  function* <Success, Error, Requirements>(
    lifecycleSemaphore: Semaphore.Semaphore,
    admissionGate: Latch.Latch,
    idle: Latch.Latch,
    transaction: Effect.Effect<Success, Error, Requirements>,
  ): Effect.fn.Return<Success, Error, Requirements> {
    return yield* lifecycleSemaphore.withPermits(1)(
      Effect.gen(function* () {
        yield* admissionGate.close;
        yield* idle.await;
        return yield* Effect.uninterruptible(transaction);
      }).pipe(Effect.ensuring(admissionGate.open)),
    );
  },
);

class EngineMutationBarrier {
  private readonly lifecycleSemaphore = Semaphore.makeUnsafe(1);
  private readonly admissionGate = Latch.makeUnsafe(true);
  private readonly idle = Latch.makeUnsafe(true);
  private activeTransactions = 0;
  private readonly tryAcquire = Effect.sync(() => {
    if (!this.admissionGate.isOpen()) {
      return false;
    }
    if (this.activeTransactions === 0) {
      this.idle.closeUnsafe();
    }
    this.activeTransactions += 1;
    return true;
  });
  private readonly release = Effect.sync(() => {
    this.activeTransactions -= 1;
    if (this.activeTransactions === 0) {
      this.idle.openUnsafe();
    }
  });

  readonly withMutation = <Success, Error, Requirements>(
    transaction: Effect.Effect<Success, Error, Requirements>,
  ): Effect.Effect<Success, Error, Requirements> =>
    Effect.acquireUseRelease(
      this.tryAcquire,
      (acquired) =>
        acquired
          ? Effect.uninterruptible(transaction)
          : this.admissionGate.await.pipe(Effect.flatMap(() => this.withMutation(transaction))),
      (acquired) => (acquired ? this.release : Effect.void),
    );

  readonly withLifecycle = <Success, Error, Requirements>(
    transaction: Effect.Effect<Success, Error, Requirements>,
  ): Effect.Effect<Success, Error, Requirements> =>
    runEngineLifecycleTransaction(
      this.lifecycleSemaphore,
      this.admissionGate,
      this.idle,
      transaction,
    );
}

class InMemoryColumnLiveViewEngine<
  Topics extends DecodableTopicDefinitions,
> implements ColumnLiveViewEngine<Topics> {
  private readonly stores = new Map<string, TopicStore>();
  private readonly groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits;
  private readonly subscriptionQueueCapacity: number;
  private readonly mutationBarrier = new EngineMutationBarrier();
  private readonly withMutationAdmission = <Success, Error, Requirements>(
    transaction: Effect.Effect<Success, Error, Requirements>,
  ): Effect.Effect<Success, Error, Requirements> => this.mutationBarrier.withMutation(transaction);
  private engineVersion = 0;
  private nextQueryId = 0;
  private closed = false;
  private readonly mutationsAllowed = (): boolean => !this.closed;

  constructor(
    config: ColumnLiveViewEngineInternalConfig<Topics>,
    private readonly topics: Topics,
  ) {
    const configuredCapacity = config.subscriptionQueueCapacity ?? defaultSubscriptionQueueCapacity;
    this.subscriptionQueueCapacity =
      Number.isSafeInteger(configuredCapacity) && configuredCapacity > 0
        ? configuredCapacity
        : defaultSubscriptionQueueCapacity;
    this.groupedIncrementalAdmissionLimits = groupedIncrementalAdmissionLimitsFromConfig(
      config.groupedIncrementalAdmissionLimits,
    );
    for (const [topic, definition] of Object.entries(this.topics)) {
      this.stores.set(
        topic,
        new TopicStore(
          topic,
          definition.schema,
          definition.key,
          () => {
            this.engineVersion += 1;
          },
          this.mutationsAllowed,
          this.withMutationAdmission,
        ),
      );
    }
  }

  private readonly getStore = Effect.fn("ColumnLiveViewEngine.store.get")(
    { self: this },
    function* <Topic extends Extract<keyof Topics, string>>(
      this: InMemoryColumnLiveViewEngine<Topics>,
      topic: Topic,
    ) {
      const store = this.stores.get(topic);
      if (store === undefined) {
        return yield* InvalidTopicError.make({
          topic,
          message: `Unknown topic: ${topic}`,
        });
      }
      return store;
    },
  );

  private readonly ensureOpen = Effect.fn("ColumnLiveViewEngine.open.ensure")(
    { self: this },
    function* (this: InMemoryColumnLiveViewEngine<Topics>) {
      if (this.closed) {
        return yield* EngineClosedError.make({
          message: "ColumnLiveViewEngine is closed.",
        });
      }
    },
  );

  readonly publish: ColumnLiveViewEngine<Topics>["publish"] = Effect.fn(
    "ColumnLiveViewEngine.publish",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, row: TopicRow<Topics, Topic>) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    yield* publishTopicStoreRow(store, row, invalidRow);
  });

  readonly publishMany: ColumnLiveViewEngine<Topics>["publishMany"] = Effect.fn(
    "ColumnLiveViewEngine.publishMany",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, rows: ReadonlyArray<TopicRow<Topics, Topic>>) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    yield* publishTopicStoreRows(store, rows, invalidRow);
  });

  readonly publishManyDecodedRows: ColumnLiveViewEngineInternal<Topics>["publishManyDecodedRows"] =
    Effect.fn("ColumnLiveViewEngine.publishManyDecodedRows")(
      { self: this },
      function* (this: InMemoryColumnLiveViewEngine<Topics>, topic, rows) {
        yield* this.ensureOpen();
        const store = yield* this.getStore(topic);
        yield* publishTopicStoreDecodedRows(store, rows, invalidRow);
      },
    );

  readonly publishManyDecodedRowsWithStorageKeys: ColumnLiveViewEngineInternal<Topics>["publishManyDecodedRowsWithStorageKeys"] =
    Effect.fn("ColumnLiveViewEngine.publishManyDecodedRowsWithStorageKeys")(
      { self: this },
      function* (this: InMemoryColumnLiveViewEngine<Topics>, topic, rows, partitionKey) {
        yield* this.ensureOpen();
        const store = yield* this.getStore(topic);
        yield* publishTopicStoreDecodedRowsWithStorageKeys(store, rows, invalidRow, partitionKey);
      },
    );

  readonly publishManyWithStorageKeys: ColumnLiveViewEngineInternal<Topics>["publishManyWithStorageKeys"] =
    Effect.fn("ColumnLiveViewEngine.publishManyWithStorageKeys")({ self: this }, function* <
      Topic extends Extract<keyof Topics, string>,
    >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, rows: Parameters<ColumnLiveViewEngineInternal<Topics>["publishManyWithStorageKeys"]>[1], partitionKey?: string) {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* publishTopicStoreRowsWithStorageKeys(store, rows, invalidRow, partitionKey);
    });

  readonly patchDecodedFields: ColumnLiveViewEngineInternal<Topics>["patchDecodedFields"] =
    Effect.fn("ColumnLiveViewEngine.patchDecodedFields")({ self: this }, function* <
      Topic extends Extract<keyof Topics, string>,
    >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, key: string, patch: object) {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* patchTopicStoreDecodedFields(store, key, patch, invalidRow);
    });

  readonly patch: ColumnLiveViewEngine<Topics>["patch"] = Effect.fn("ColumnLiveViewEngine.patch")(
    { self: this },
    function* <
      Topic extends Extract<keyof Topics, string>,
      const Patch extends Partial<TopicRow<Topics, Topic>>,
    >(
      this: InMemoryColumnLiveViewEngine<Topics>,
      topic: Topic,
      key: string,
      patch: ExactPatch<TopicRow<Topics, Topic>, Patch>,
    ) {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* patchTopicStoreRow(store, key, patch, invalidRow);
    },
  );

  readonly delete: ColumnLiveViewEngine<Topics>["delete"] = Effect.fn(
    "ColumnLiveViewEngine.delete",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, key: string) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    yield* deleteTopicStoreRow(store, key);
  });

  readonly deleteStorageKey: ColumnLiveViewEngineInternal<Topics>["deleteStorageKey"] = Effect.fn(
    "ColumnLiveViewEngine.deleteStorageKey",
  )(
    { self: this },
    function* (this: InMemoryColumnLiveViewEngine<Topics>, topic, key, partitionKey) {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* deleteTopicStoreRow(store, key, partitionKey);
    },
  );

  snapshot<Topic extends Extract<keyof Topics, string>, const Query>(
    topic: Topic,
    query: ExactEngineLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  snapshot<Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: object,
  ): Effect.Effect<LiveQueryResult<object>, ColumnLiveViewEngineError> {
    const capturedQuery = Result.try(() => snapshotViewServerQuery(query));
    return Effect.fn("ColumnLiveViewEngine.snapshot")(
      { self: this },
      function* (this: InMemoryColumnLiveViewEngine<Topics>) {
        if (Result.isFailure(capturedQuery)) {
          return yield* InvalidQueryError.make({
            topic,
            message: viewServerQuerySnapshotErrorMessage,
          });
        }
        return yield* this.snapshotRuntime(topic, capturedQuery.success);
      },
    )();
  }

  readonly snapshotRuntime: ColumnLiveViewEngineInternal<Topics>["snapshotRuntime"] = (
    topic,
    query,
  ) =>
    Effect.gen({ self: this }, function* (this: InMemoryColumnLiveViewEngine<Topics>) {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      return yield* snapshotRuntimeExecutableQuery(store, query);
    });

  private readonly subscribeRuntimeQuery = <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: unknown,
    terminalObserver: ColumnLiveViewTerminalObserver,
    partition?: ColumnLiveViewEngineQueryPartition,
  ) => {
    const capturedQuery = Result.try(() => snapshotViewServerQuery(query));
    return Effect.fn("ColumnLiveViewEngine.subscribeRuntime")(
      { self: this },
      function* (this: InMemoryColumnLiveViewEngine<Topics>) {
        if (Result.isFailure(capturedQuery)) {
          return yield* InvalidQueryError.make({
            topic,
            message: viewServerQuerySnapshotErrorMessage,
          });
        }
        yield* this.ensureOpen();
        const store = yield* this.getStore(topic);
        const subscription = yield* acquireTopicStoreSubscription(
          store,
          (
            permit,
            markAcquired: (subscription: LiveSubscription<object>) => Effect.Effect<void>,
          ): Effect.Effect<LiveSubscription<object>, ColumnLiveViewEngineError> =>
            Effect.gen({ self: this }, function* () {
              yield* this.ensureOpen();
              const queryId = `query-${this.nextQueryId}`;
              this.nextQueryId += 1;
              const acquiredSubscription = yield* subscribeRuntimeExecutableQuery(
                capturedQuery.success,
                {
                  groupedIncrementalAdmissionLimits: this.groupedIncrementalAdmissionLimits,
                  permit,
                  queryId,
                  queueCapacity: this.subscriptionQueueCapacity,
                  terminalObserver,
                },
                partition,
              );
              yield* markAcquired(acquiredSubscription);
              return acquiredSubscription;
            }),
        );

        return {
          events: subscription.events,
          close: subscription.close,
        };
      },
    )();
  };

  subscribe<Topic extends Extract<keyof Topics, string>, const Query>(
    topic: Topic,
    query: ExactEngineLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  subscribe<Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: object,
  ): Effect.Effect<ColumnLiveViewSubscription<object>, ColumnLiveViewEngineError> {
    return this.subscribeRuntimeQuery(topic, query, unobservedTerminal);
  }

  readonly subscribeRuntime: ColumnLiveViewEngine<Topics>["subscribeRuntime"] = (topic, query) =>
    this.subscribeRuntimeQuery(topic, query, unobservedTerminal);

  subscribeObserved<Topic extends Extract<keyof Topics, string>, const Query>(
    topic: Topic,
    query: ExactEngineLiveQueryInputForTopic<Topics, NoInfer<Topic>, Query>,
    observer: ColumnLiveViewTerminalObserver,
  ): Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  subscribeObserved<Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: object,
    observer: ColumnLiveViewTerminalObserver,
  ): Effect.Effect<ColumnLiveViewSubscription<object>, ColumnLiveViewEngineError> {
    return this.subscribeRuntimeQuery(topic, query, observer);
  }

  readonly subscribeRuntimeObserved: ColumnLiveViewEngineInternal<Topics>["subscribeRuntimeObserved"] =
    (topic, query, observer) => this.subscribeRuntimeQuery(topic, query, observer);

  readonly subscribeRuntimePartitioned: ColumnLiveViewEngineInternal<Topics>["subscribeRuntimePartitioned"] =
    (topic, query, partition) =>
      this.subscribeRuntimeQuery(topic, query, unobservedTerminal, partition);

  readonly subscribeRuntimeObservedPartitioned: ColumnLiveViewEngineInternal<Topics>["subscribeRuntimeObservedPartitioned"] =
    (topic, query, partition, observer) =>
      this.subscribeRuntimeQuery(topic, query, observer, partition);

  readonly health: ColumnLiveViewEngine<Topics>["health"] = Effect.fn(
    "ColumnLiveViewEngine.health",
  )({ self: this }, function* (this: InMemoryColumnLiveViewEngine<Topics>) {
    return yield* collectColumnLiveViewEngineHealth<Topics>(this.stores, {
      version: () => this.engineVersion,
      closed: () => this.closed,
    });
  });

  readonly reset: ColumnLiveViewEngine<Topics>["reset"] = Effect.fn("ColumnLiveViewEngine.reset")(
    { self: this },
    function* (this: InMemoryColumnLiveViewEngine<Topics>) {
      yield* this.mutationBarrier.withLifecycle(
        Effect.gen({ self: this }, function* () {
          yield* this.ensureOpen();
          for (const store of this.stores.values()) {
            yield* resetTopicStore(store);
          }
          this.engineVersion = 0;
        }),
      );
    },
  );

  readonly close: ColumnLiveViewEngine<Topics>["close"] = Effect.fn("ColumnLiveViewEngine.close")(
    { self: this },
    function* (this: InMemoryColumnLiveViewEngine<Topics>) {
      yield* this.mutationBarrier.withLifecycle(
        Effect.gen({ self: this }, function* () {
          this.closed = true;
          for (const store of this.stores.values()) {
            yield* closeTopicStoreSubscriptions(store);
          }
        }),
      );
    },
  );
}

const publicColumnLiveViewEngine = <Topics extends DecodableTopicDefinitions>(
  engine: InMemoryColumnLiveViewEngine<Topics>,
): ColumnLiveViewEngine<Topics> => ({
  close: engine.close,
  delete: engine.delete,
  health: engine.health,
  patch: engine.patch,
  publish: engine.publish,
  publishMany: engine.publishMany,
  reset: engine.reset,
  snapshot: engine.snapshot.bind(engine),
  subscribe: engine.subscribe.bind(engine),
  subscribeRuntime: engine.subscribeRuntime,
});

export const createColumnLiveViewEngine = Effect.fn("ColumnLiveViewEngine.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ColumnLiveViewEngineConfig<Topics>,
  ): Effect.Effect<ColumnLiveViewEngine<Topics>, InvalidRowError> =>
    Effect.gen(function* () {
      const topics = yield* snapshotAndValidateEngineTopics(config.topics);
      return publicColumnLiveViewEngine(new InMemoryColumnLiveViewEngine(config, topics));
    }),
);

export const createColumnLiveViewEngineInternal = Effect.fn("ColumnLiveViewEngine.internal.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ColumnLiveViewEngineInternalConfig<Topics>,
  ): Effect.Effect<ColumnLiveViewEngineInternal<Topics>, InvalidRowError> =>
    Effect.gen(function* () {
      const topics = yield* snapshotAndValidateEngineTopics(config.topics);
      return new InMemoryColumnLiveViewEngine(config, topics);
    }),
);
