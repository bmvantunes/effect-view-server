import { Clock, Effect, Schema, Semaphore } from "effect";
import type { StatusEvent, TopicRuntimeHealth } from "@view-server/config";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
  clearStoreRawQueryExecutions,
  evaluateRawQuery,
  releaseMaterializedQueryExecution,
  releaseRawQueryExecution,
  type ActiveQueryStoreState,
} from "./active-query";
import { ColumnarTopicStore, type PreparedTopicRow } from "./columnar-topic-store";
import {
  evaluateCompiledGroupedQuery,
  prepareGroupedQuery,
  type CompiledGroupedQuery,
} from "./grouped-query-compiler";
import { createTopicHealthLedger } from "./topic-health-ledger";
import {
  prepareRawQuery,
  type CompiledRawQuery,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import {
  acquireSubscriptionHandoff,
  type MarkAcquiredSubscription,
  type SubscriptionHandoffOptions,
} from "./subscription-handoff";
import type { LiveTopicSubscriber } from "./topic-subscriber";

type RowObject = object;

type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;
const topicStoreSubscriptionPermitBrand: unique symbol = Symbol("TopicStoreSubscriptionPermit");

export type TopicStoreSubscriptionPermit = {
  readonly [topicStoreSubscriptionPermitBrand]: true;
  readonly store: TopicStore;
};

export type TopicStoreHealthView = {
  readonly topic: string;
  readonly status: "ready" | "degraded";
  readonly rowCount: number;
  readonly liveRowCount: number;
  readonly deletedRowCount: number;
  readonly version: number;
  readonly lastMutationAt: number | null;
  readonly mutationsPerSecond: number;
  readonly rowsPerSecond: number;
  readonly pendingMutationBatches: number;
  readonly activeViews: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
};

type TopicStoreState = {
  readonly storage: ColumnarTopicStore;
  readonly subscribers: Set<LiveTopicSubscriber>;
  readonly mutationSemaphore: Semaphore.Semaphore;
  readonly notificationSemaphore: Semaphore.Semaphore;
  readonly healthLedger: ReturnType<typeof createTopicHealthLedger>;
  readonly onCommit: () => void;
};

type TopicStoreMutationContext = {
  readonly publishPrepared: (prepared: PreparedTopicRow) => number;
  readonly publishPreparedMany: (preparedRows: ReadonlyArray<PreparedTopicRow>) => number;
  readonly patch: <Patch extends Partial<RowObject>, Error>(
    key: string,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) => Effect.Effect<number, Error>;
  readonly delete: (key: string) => number;
};

const topicStoreStates = new WeakMap<TopicStore, TopicStoreState>();

export class TopicStore {
  declare private readonly topicStoreBrand: void;

  constructor(
    readonly topic: string,
    schema: Schema.Decoder<object>,
    keyField: string,
    onCommit: () => void,
  ) {
    const storage = new ColumnarTopicStore(topic, schema, keyField);
    const subscribers = new Set<LiveTopicSubscriber>();
    const state: TopicStoreState = {
      storage,
      subscribers,
      mutationSemaphore: Semaphore.makeUnsafe(1),
      notificationSemaphore: Semaphore.makeUnsafe(1),
      healthLedger: createTopicHealthLedger(),
      onCommit,
    };
    topicStoreStates.set(this, state);
  }
}

const topicStoreState = (store: TopicStore): TopicStoreState => {
  return topicStoreStates.get(store)!;
};

export const topicStoreRawQueryMetadata = (store: TopicStore): RawQueryCompilerMetadata =>
  topicStoreState(store).storage.rawQueryMetadata;

export const topicStoreReadModel = (store: TopicStore): ActiveQueryStoreState =>
  topicStoreState(store).storage.readModel;

export const prepareTopicStoreRawQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.prepare",
)(function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
  return yield* prepareRawQuery<object, ResultRow>(
    store.topic,
    topicStoreState(store).storage.rawQueryMetadata,
    query,
  );
});

export const prepareTopicStoreGroupedQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.grouped.prepare",
)(function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
  return yield* prepareGroupedQuery<object, ResultRow>(
    store.topic,
    topicStoreState(store).storage.rawQueryMetadata,
    query,
  );
});

export const evaluateTopicStoreRawQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): QueryEvaluation<ResultRow> =>
  evaluateRawQuery(topicStoreState(store).storage.readModel, compiled);

export const evaluateTopicStoreGroupedQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): QueryEvaluation<ResultRow> =>
  evaluateCompiledGroupedQuery(topicStoreState(store).storage.readModel, compiled);

export const acquireTopicStoreRawQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
) {
  return yield* acquireRawQueryExecution(topicStoreState(store).storage.readModel, compiled);
});

export const releaseTopicStoreRawQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseRawQueryExecution(topicStoreState(store).storage.readModel, compiled);

export const acquireTopicStoreMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.materialized.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
) {
  const readModel = topicStoreState(store).storage.readModel;
  return yield* acquireMaterializedQueryExecution(readModel, compiled.cacheKey, () =>
    evaluateCompiledGroupedQuery(readModel, compiled),
  );
});

export const releaseTopicStoreMaterializedQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseMaterializedQueryExecution(topicStoreState(store).storage.readModel, compiled.cacheKey);

const withTopicStoreTransaction = Effect.fn("ColumnLiveViewEngine.topicStore.transaction")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* topicStoreState(store).mutationSemaphore.withPermits(1)(
      Effect.uninterruptible(effect),
    );
  },
);

const withTopicStoreNotification = Effect.fn("ColumnLiveViewEngine.topicStore.notification")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* topicStoreState(store).notificationSemaphore.withPermits(1)(
      Effect.uninterruptible(effect),
    );
  },
);

export function acquireTopicStoreSubscription<
  Subscription extends { readonly close: () => Effect.Effect<void, never> },
  Error,
  Requirements,
>(
  store: TopicStore,
  acquire: (
    permit: TopicStoreSubscriptionPermit,
    markAcquired: MarkAcquiredSubscription<Subscription>,
  ) => Effect.Effect<Subscription, Error, Requirements>,
  options: SubscriptionHandoffOptions = {},
): Effect.Effect<Subscription, Error, Requirements> {
  return acquireSubscriptionHandoff(
    (markAcquired: (subscription: Subscription) => Effect.Effect<void>) =>
      withTopicStoreTransaction(
        store,
        acquire(
          {
            [topicStoreSubscriptionPermitBrand]: true,
            store,
          },
          markAcquired,
        ),
      ),
    options,
  );
}

const resetStatusEvent = (store: TopicStore, subscriber: LiveTopicSubscriber): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "SubscriptionClosed",
  message: "Subscription closed because the engine reset.",
});

const engineClosedStatusEvent = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "SubscriptionClosed",
  message: "Subscription closed because the engine closed.",
});

const commitTopicStoreState = (state: TopicStoreState): ReadonlyArray<LiveTopicSubscriber> => {
  state.storage.advanceVersion();
  state.onCommit();
  return [...state.subscribers];
};

const recordTopicStoreMutation = (
  state: TopicStoreState,
  rowsChanged: number,
  occurredAt: number,
): ReadonlyArray<LiveTopicSubscriber> => {
  const subscribersToNotify = commitTopicStoreState(state);
  state.healthLedger.recordMutation({
    version: state.storage.version,
    rowCount: state.storage.rowCount,
    rowsChanged,
    occurredAt,
  });
  return subscribersToNotify;
};

const topicStoreMutationContext = (state: TopicStoreState): TopicStoreMutationContext => ({
  publishPrepared: (prepared) => {
    state.storage.setPrepared(prepared);
    return 1;
  },
  publishPreparedMany: (preparedRows) => {
    state.storage.setPreparedMany(preparedRows);
    return preparedRows.length;
  },
  patch: (key, patch, invalidRow) =>
    Effect.gen(function* () {
      const prepared = yield* state.storage.preparePatch(key, patch, invalidRow);
      state.storage.setPrepared(prepared);
      return 1;
    }),
  delete: (key) => state.storage.delete(key),
});

const notifyTopicStoreSubscribers = Effect.fn("ColumnLiveViewEngine.topicStore.notify")(function* (
  store: TopicStore,
  subscribers: ReadonlyArray<LiveTopicSubscriber>,
) {
  yield* withTopicStoreNotification(
    store,
    Effect.gen(function* () {
      for (const subscriber of subscribers) {
        if (!subscriber.closed) {
          yield* subscriber.notify(store);
        }
      }
    }),
  );
});

const runTopicStoreMutationTransaction = Effect.fn(
  "ColumnLiveViewEngine.topicStore.mutationTransaction",
)(function* <Error, Requirements>(
  store: TopicStore,
  mutate: (mutation: TopicStoreMutationContext) => Effect.Effect<number, Error, Requirements>,
) {
  yield* withTopicStoreMutationBatch(
    store,
    Effect.gen(function* () {
      const subscribers = yield* withTopicStoreTransaction(
        store,
        Effect.gen(function* () {
          const state = topicStoreState(store);
          const rowsChanged = yield* mutate(topicStoreMutationContext(state));
          const occurredAt = yield* Clock.currentTimeMillis;
          return recordTopicStoreMutation(state, rowsChanged, occurredAt);
        }),
      );
      yield* notifyTopicStoreSubscribers(store, subscribers);
    }),
  );
});

const withTopicStoreMutationBatch = Effect.fn("ColumnLiveViewEngine.topicStore.mutationBatch")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    const ledger = topicStoreState(store).healthLedger;
    return yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        ledger.beginMutationBatch();
      }),
      () => effect,
      () =>
        Effect.sync(() => {
          ledger.endMutationBatch();
        }),
    );
  },
);

export const collectTopicStoreHealth = Effect.fn("ColumnLiveViewEngine.topicStore.health")(
  function* (store: TopicStore, closed: boolean) {
    const state = topicStoreState(store);
    const totals = state.healthLedger.snapshot(yield* Clock.currentTimeMillis);
    let queuedEvents = 0;

    for (const subscriber of state.subscribers) {
      const currentQueuedEvents = yield* subscriber.queuedEvents;
      queuedEvents += currentQueuedEvents;
    }

    const activeSubscriptions = state.subscribers.size;
    const activeViews = yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store));
    const status: TopicRuntimeHealth["status"] = closed ? "degraded" : "ready";
    const lastMutationAt = totals.lastMutationAt;
    const rowsPerSecond = totals.rowsPerSecond;
    const health: TopicStoreHealthView = {
      topic: store.topic,
      status,
      rowCount: totals.rowCount,
      liveRowCount: totals.rowCount,
      deletedRowCount: 0,
      version: totals.version,
      lastMutationAt,
      mutationsPerSecond: totals.mutationsPerSecond,
      rowsPerSecond,
      pendingMutationBatches: totals.pendingMutationBatches,
      activeViews,
      activeSubscriptions,
      queuedEvents,
      maxQueueDepth: totals.maxQueueDepth,
      backpressureEvents: totals.backpressureEvents,
      memoryBytes: 0,
      tombstoneCount: 0,
      compactionPending: false,
    };
    return health;
  },
);

export const registerTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.add",
)(function (permit: TopicStoreSubscriptionPermit, subscriber: LiveTopicSubscriber) {
  return Effect.sync(() => {
    const state = topicStoreState(permit.store);
    state.healthLedger.openSubscription(subscriber);
    state.subscribers.add(subscriber);
  });
});

const unregisterTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.remove",
)(function (store: TopicStore, subscriber: LiveTopicSubscriber) {
  return Effect.sync(() => {
    const state = topicStoreState(store);
    state.healthLedger.closeSubscription(subscriber);
    state.subscribers.delete(subscriber);
  });
});

const drainTopicStoreSubscribersForReset = (
  state: TopicStoreState,
): ReadonlyArray<LiveTopicSubscriber> => {
  state.storage.clear();
  const closingSubscribers = [...state.subscribers];
  for (const subscriber of closingSubscribers) {
    subscriber.closed = true;
  }
  state.subscribers.clear();
  state.healthLedger.reset();
  return closingSubscribers;
};

function drainTopicStoreSubscribersForClose(
  state: TopicStoreState,
): ReadonlyArray<LiveTopicSubscriber> {
  const closingSubscribers = Array.from(state.subscribers);
  for (const subscriber of closingSubscribers) {
    subscriber.closed = true;
    state.healthLedger.closeSubscription(subscriber);
  }
  state.subscribers.clear();
  return closingSubscribers;
}

export const trackTopicStoreSubscriptionQueueDepth = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.queueDepth",
)((store: TopicStore, subscriber: LiveTopicSubscriber, queueDepth: number) =>
  Effect.sync(() => {
    topicStoreState(store).healthLedger.updateQueueDepth(subscriber, queueDepth);
    subscriber.maxQueueDepth = Math.max(subscriber.maxQueueDepth, queueDepth);
  }),
);

const reportTopicStoreSubscriptionBackpressure = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.backpressure",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    topicStoreState(store).healthLedger.markBackpressure(subscriber);
    subscriber.backpressureEvents += 1;
  }),
);

export const closeTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.close",
)(function* (store: TopicStore, subscriber: LiveTopicSubscriber, finalize: Effect.Effect<void>) {
  yield* withTopicStoreNotification(
    store,
    withTopicStoreTransaction(
      store,
      Effect.gen(function* () {
        if (subscriber.closed) {
          return;
        }
        subscriber.closed = true;
        yield* unregisterTopicStoreSubscription(store, subscriber);
        yield* finalize;
      }),
    ),
  );
});

export const closeBackpressuredTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.closeBackpressured",
)(function* (store: TopicStore, subscriber: LiveTopicSubscriber, finalize: Effect.Effect<void>) {
  yield* withTopicStoreTransaction(
    store,
    Effect.gen(function* () {
      if (subscriber.closed) {
        return;
      }
      subscriber.closed = true;
      yield* reportTopicStoreSubscriptionBackpressure(store, subscriber);
      yield* unregisterTopicStoreSubscription(store, subscriber);
      yield* finalize;
    }),
  );
});

export const resetTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.reset")(function* (
  store: TopicStore,
) {
  yield* withTopicStoreNotification(
    store,
    withTopicStoreTransaction(
      store,
      Effect.gen(function* () {
        const subscribers = yield* Effect.sync(() => {
          return drainTopicStoreSubscribersForReset(topicStoreState(store));
        });
        yield* clearStoreRawQueryExecutions(topicStoreReadModel(store));
        for (const subscriber of subscribers) {
          yield* subscriber.closeWithStatus(resetStatusEvent(store, subscriber));
        }
      }),
    ),
  );
});

export const closeTopicStoreSubscriptions = Effect.fn(
  "ColumnLiveViewEngine.topicStore.closeSubscriptions",
)(function* (store: TopicStore) {
  yield* withTopicStoreNotification(
    store,
    withTopicStoreTransaction(
      store,
      Effect.gen(function* () {
        const subscribers = yield* Effect.sync(() => {
          return drainTopicStoreSubscribersForClose(topicStoreState(store));
        });
        yield* clearStoreRawQueryExecutions(topicStoreReadModel(store));
        for (const subscriber of subscribers) {
          yield* subscriber.closeWithStatus(engineClosedStatusEvent(store, subscriber));
        }
      }),
    ),
  );
});

export const publishTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.publish")(function* <
  Error,
  Row extends RowObject,
>(store: TopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const prepared = yield* topicStoreState(store).storage.prepareRow(row, invalidRow);
  yield* runTopicStoreMutationTransaction(store, (mutation) =>
    Effect.sync(() => {
      return mutation.publishPrepared(prepared);
    }),
  );
});

export const publishTopicStoreRows = Effect.fn("ColumnLiveViewEngine.topicStore.publishMany")(
  function* <Error, Row extends RowObject>(
    store: TopicStore,
    rows: ReadonlyArray<Row>,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    const preparedRows = yield* topicStoreState(store).storage.prepareRows(rows, invalidRow);
    yield* runTopicStoreMutationTransaction(store, (mutation) =>
      Effect.sync(() => {
        return mutation.publishPreparedMany(preparedRows);
      }),
    );
  },
);

export const patchTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.patch")(function* <
  Patch extends Partial<RowObject>,
  Error,
>(store: TopicStore, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
  yield* runTopicStoreMutationTransaction(store, (mutation) =>
    mutation.patch(key, patch, invalidRow),
  );
});

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* (
  store: TopicStore,
  key: string,
) {
  yield* runTopicStoreMutationTransaction(store, (mutation) =>
    Effect.sync(() => {
      return mutation.delete(key);
    }),
  );
});
