import { Clock, Effect, Semaphore } from "effect";
import { EngineClosedError } from "./engine-errors";
import type { TopicRowStorage } from "./topic-row-storage";
import type { InvalidRowErrorFactory, PreparedTopicRow } from "./topic-row-preparation";
import type { createTopicHealthLedger } from "./topic-health-ledger";
import type { LiveTopicSubscriber } from "./topic-subscriber";
import { topicStoreState, type TopicStore } from "./topic-store-state";

type RowObject = object;

export type TopicStoreMutationState = {
  readonly storage: TopicRowStorage;
  readonly subscribers: Set<LiveTopicSubscriber>;
  readonly partitionedSubscribers: Map<string, Set<LiveTopicSubscriber>>;
  readonly unpartitionedSubscribers: Set<LiveTopicSubscriber>;
  readonly mutationSemaphore: Semaphore.Semaphore;
  readonly notificationSemaphore: Semaphore.Semaphore;
  readonly healthLedger: ReturnType<typeof createTopicHealthLedger>;
  readonly mutationsAllowed: () => boolean;
  readonly onCommit: () => void;
  readonly withMutationAdmission: TopicStoreMutationAdmission;
};

export type TopicStoreMutationAdmission = <Success, Error, Requirements>(
  transaction: Effect.Effect<Success, Error, Requirements>,
) => Effect.Effect<Success, Error, Requirements>;

export type TopicStoreMutationContext = {
  readonly publishPrepared: (prepared: PreparedTopicRow, partitionKey?: string) => number;
  readonly publishPreparedMany: (
    preparedRows: ReadonlyArray<PreparedTopicRow>,
    partitionKey?: string,
  ) => number;
  readonly patch: <Patch extends Partial<RowObject>, Error>(
    key: string,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) => Effect.Effect<number, Error>;
  readonly patchDecoded: <Patch extends Partial<RowObject>, Error>(
    key: string,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) => Effect.Effect<number, Error>;
  readonly delete: (key: string, partitionKey?: string) => number;
};

export type TopicStoreRowWithStorageKey<Row extends object> = {
  readonly storageKey: string;
  readonly row: Row;
};

const withTopicStoreStateTransaction = <Success, Error, Requirements>(
  state: TopicStoreMutationState,
  effect: Effect.Effect<Success, Error, Requirements>,
): Effect.Effect<Success, Error, Requirements> =>
  state.mutationSemaphore.withPermits(1)(Effect.uninterruptible(effect));

const withTopicStoreStateNotification = <Success, Error, Requirements>(
  state: TopicStoreMutationState,
  effect: Effect.Effect<Success, Error, Requirements>,
): Effect.Effect<Success, Error, Requirements> =>
  state.notificationSemaphore.withPermits(1)(Effect.uninterruptible(effect));

export const withTopicStoreTransaction = Effect.fn("ColumnLiveViewEngine.topicStore.transaction")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* withTopicStoreStateTransaction(topicStoreState(store), effect);
  },
);

export const withTopicStoreNotification = Effect.fn("ColumnLiveViewEngine.topicStore.notification")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* withTopicStoreStateNotification(topicStoreState(store), effect);
  },
);

export const withTopicStoreStateTransition = Effect.fn(
  "ColumnLiveViewEngine.topicStore.stateTransition",
)(function* <Success, Error, Requirements>(
  store: TopicStore,
  transition: (state: TopicStoreMutationState) => Effect.Effect<Success, Error, Requirements>,
) {
  const state = topicStoreState(store);
  return yield* withTopicStoreStateNotification(
    state,
    withTopicStoreStateTransaction(state, transition(state)),
  );
});

const commitTopicStoreState = (
  state: TopicStoreMutationState,
  partitionKey?: string,
): ReadonlyArray<LiveTopicSubscriber> => {
  state.storage.advanceVersion(partitionKey);
  state.onCommit();
  if (partitionKey === undefined) {
    return [...state.subscribers];
  }
  return [
    ...state.unpartitionedSubscribers,
    ...(state.partitionedSubscribers.get(partitionKey) ?? []),
  ];
};

const recordTopicStoreMutation = (
  state: TopicStoreMutationState,
  rowsChanged: number,
  occurredAt: number,
  partitionKey?: string,
): ReadonlyArray<LiveTopicSubscriber> => {
  if (rowsChanged <= 0) {
    return [];
  }
  const subscribersToNotify = commitTopicStoreState(state, partitionKey);
  state.healthLedger.recordMutation({
    version: state.storage.version,
    rowCount: state.storage.rowCount,
    rowsChanged,
    occurredAt,
  });
  return subscribersToNotify;
};

const topicStoreMutationContext = (state: TopicStoreMutationState): TopicStoreMutationContext => ({
  publishPrepared: (prepared, partitionKey) => state.storage.setPrepared(prepared, partitionKey),
  publishPreparedMany: (preparedRows, partitionKey) =>
    state.storage.setPreparedMany(preparedRows, partitionKey),
  patch: (key, patch, invalidRow) =>
    Effect.gen(function* () {
      const prepared = yield* state.storage.preparePatch(key, patch, invalidRow);
      return state.storage.setPrepared(prepared);
    }),
  patchDecoded: (key, patch, invalidRow) =>
    Effect.gen(function* () {
      const prepared = yield* state.storage.prepareDecodedPatch(key, patch, invalidRow);
      return state.storage.setPrepared(prepared);
    }),
  delete: (key, partitionKey) => state.storage.delete(key, partitionKey),
});

const withTopicStoreMutationBatch = Effect.fn("ColumnLiveViewEngine.topicStore.mutationBatch")(
  function* <Success, Error, Requirements>(
    state: TopicStoreMutationState,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    const ledger = state.healthLedger;
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

const notifyTopicStoreSubscribers = Effect.fn("ColumnLiveViewEngine.topicStore.notify")(function* (
  state: TopicStoreMutationState,
  store: TopicStore,
  subscribers: ReadonlyArray<LiveTopicSubscriber>,
) {
  yield* withTopicStoreStateNotification(
    state,
    Effect.gen(function* () {
      for (const subscriber of subscribers) {
        if (!subscriber.closed) {
          yield* subscriber.notify(store);
        }
      }
    }),
  );
});

export const runTopicStoreMutationTransaction = Effect.fn(
  "ColumnLiveViewEngine.topicStore.mutationTransaction",
)(function* <Error, Requirements>(
  state: TopicStoreMutationState,
  store: TopicStore,
  mutate: (mutation: TopicStoreMutationContext) => Effect.Effect<number, Error, Requirements>,
  partitionKey?: string,
) {
  yield* state.withMutationAdmission(
    withTopicStoreMutationBatch(
      state,
      Effect.gen(function* () {
        const subscribers = yield* withTopicStoreStateTransaction(
          state,
          Effect.gen(function* () {
            if (!state.mutationsAllowed()) {
              return yield* EngineClosedError.make({
                message: "ColumnLiveViewEngine is closed.",
              });
            }
            const rowsChanged = yield* mutate(topicStoreMutationContext(state));
            const occurredAt = yield* Clock.currentTimeMillis;
            return recordTopicStoreMutation(state, rowsChanged, occurredAt, partitionKey);
          }),
        );
        yield* notifyTopicStoreSubscribers(state, store, subscribers);
      }),
    ),
  );
});

export const publishTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.publish")(function* <
  Error,
  Row extends RowObject,
>(store: TopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const state = topicStoreState(store);
  const prepared = yield* state.storage.prepareRow(row, invalidRow);
  yield* runTopicStoreMutationTransaction(state, store, (mutation) =>
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
    const state = topicStoreState(store);
    const preparedRows = yield* state.storage.prepareRows(rows, invalidRow);
    yield* runTopicStoreMutationTransaction(state, store, (mutation) =>
      Effect.sync(() => {
        return mutation.publishPreparedMany(preparedRows);
      }),
    );
  },
);

export const publishTopicStoreDecodedRows = Effect.fn(
  "ColumnLiveViewEngine.topicStore.publishManyDecoded",
)(function* <Error, Row extends RowObject>(
  store: TopicStore,
  rows: ReadonlyArray<Row>,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  const state = topicStoreState(store);
  const preparedRows = yield* state.storage.prepareDecodedRows(rows, invalidRow);
  yield* runTopicStoreMutationTransaction(state, store, (mutation) =>
    Effect.sync(() => {
      return mutation.publishPreparedMany(preparedRows);
    }),
  );
});

export const publishTopicStoreRowsWithStorageKeys = Effect.fn(
  "ColumnLiveViewEngine.topicStore.publishManyWithStorageKeys",
)(function* <Error, Row extends RowObject>(
  store: TopicStore,
  rows: ReadonlyArray<TopicStoreRowWithStorageKey<Row>>,
  invalidRow: InvalidRowErrorFactory<Error>,
  partitionKey?: string,
) {
  const state = topicStoreState(store);
  const preparedRows = yield* Effect.forEach(rows, (entry) =>
    state.storage.prepareRowWithStorageKey(entry.row, entry.storageKey, invalidRow),
  );
  yield* runTopicStoreMutationTransaction(
    state,
    store,
    (mutation) =>
      Effect.sync(() => {
        return mutation.publishPreparedMany(preparedRows, partitionKey);
      }),
    partitionKey,
  );
});

export const publishTopicStoreDecodedRowsWithStorageKeys = Effect.fn(
  "ColumnLiveViewEngine.topicStore.publishManyDecodedWithStorageKeys",
)(function* <Error, Row extends RowObject>(
  store: TopicStore,
  rows: ReadonlyArray<TopicStoreRowWithStorageKey<Row>>,
  invalidRow: InvalidRowErrorFactory<Error>,
  partitionKey?: string,
) {
  const state = topicStoreState(store);
  const preparedRows = yield* Effect.forEach(rows, (entry) =>
    state.storage.prepareDecodedRowWithStorageKey(entry.row, entry.storageKey, invalidRow),
  );
  yield* runTopicStoreMutationTransaction(
    state,
    store,
    (mutation) =>
      Effect.sync(() => {
        return mutation.publishPreparedMany(preparedRows, partitionKey);
      }),
    partitionKey,
  );
});

export const patchTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.patch")(function* <
  Patch extends Partial<RowObject>,
  Error,
>(store: TopicStore, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
  yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
    mutation.patch(key, patch, invalidRow),
  );
});

export const patchTopicStoreDecodedFields = Effect.fn(
  "ColumnLiveViewEngine.topicStore.patchDecodedFields",
)(function* <Patch extends Partial<RowObject>, Error>(
  store: TopicStore,
  key: string,
  patch: Patch,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
    mutation.patchDecoded(key, patch, invalidRow),
  );
});

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* (
  store: TopicStore,
  key: string,
  partitionKey?: string,
) {
  yield* runTopicStoreMutationTransaction(
    topicStoreState(store),
    store,
    (mutation) =>
      Effect.sync(() => {
        return mutation.delete(key, partitionKey);
      }),
    partitionKey,
  );
});
