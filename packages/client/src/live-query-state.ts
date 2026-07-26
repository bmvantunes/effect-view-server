import type { DeltaEvent, LiveQueryResult } from "@effect-view-server/config";
import type { ViewServerLiveEvent } from "./live-client";

export type ClientState<Row> = {
  readonly rows: ReadonlyArray<Row>;
  readonly keys: ReadonlyArray<string>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: LiveQueryResult<Row>["status"];
  readonly statusCode?: LiveQueryResult<Row>["statusCode"];
  readonly message?: string | undefined;
};

export const initialClientState = <Row>(): ClientState<Row> => ({
  rows: [],
  keys: [],
  totalRows: 0,
  version: 0,
  status: "loading",
});

export const liveQueryResult = <Row>(state: ClientState<Row>): LiveQueryResult<Row> => ({
  rows: state.rows,
  totalRows: state.totalRows,
  version: state.version,
  status: state.status,
  statusCode: state.statusCode,
  message: state.message,
});

const reindexKeys = (
  keys: ReadonlyArray<string>,
  keyIndexes: Map<string, number>,
  startIndex: number,
): void => {
  for (let index = startIndex; index < keys.length; index += 1) {
    keyIndexes.set(keys[index]!, index);
  }
};

const isInsertIndex = (index: number, length: number): boolean =>
  Number.isSafeInteger(index) && index >= 0 && index <= length;

const isExistingIndex = (index: number, length: number): boolean =>
  Number.isSafeInteger(index) && index >= 0 && index < length;

const isNonNegativeSafeInteger = (value: number): boolean =>
  Number.isSafeInteger(value) && value >= 0;

const hasUniqueKeys = (keys: ReadonlyArray<string>): boolean => {
  const seenKeys = new Set<string>();
  for (const key of keys) {
    if (seenKeys.has(key)) {
      return false;
    }
    seenKeys.add(key);
  }
  return true;
};

const isValidSnapshotEvent = <Row>(
  event: Extract<ViewServerLiveEvent<Row>, { readonly type: "snapshot" }>,
): boolean =>
  isNonNegativeSafeInteger(event.version) &&
  isNonNegativeSafeInteger(event.totalRows) &&
  event.totalRows >= event.rows.length &&
  event.keys.length === event.rows.length &&
  hasUniqueKeys(event.keys);

type ClientStateMutation<Row> =
  | {
      readonly _tag: "Insert";
      readonly index: number;
      readonly key: string;
    }
  | {
      readonly _tag: "Update";
      readonly index: number;
      readonly row: Row;
    }
  | {
      readonly _tag: "Move";
      readonly fromIndex: number;
      readonly toIndex: number;
    }
  | {
      readonly _tag: "Remove";
      readonly index: number;
      readonly key: string;
      readonly row: Row;
    };

const rollbackMutations = <Row>(
  rows: Array<Row>,
  keys: Array<string>,
  keyIndexes: Map<string, number>,
  mutations: ReadonlyArray<ClientStateMutation<Row>>,
): void => {
  for (let mutationIndex = mutations.length - 1; mutationIndex >= 0; mutationIndex -= 1) {
    const mutation = mutations[mutationIndex]!;
    if (mutation._tag === "Insert") {
      rows.splice(mutation.index, 1);
      keys.splice(mutation.index, 1);
      keyIndexes.delete(mutation.key);
      reindexKeys(keys, keyIndexes, mutation.index);
    } else if (mutation._tag === "Update") {
      rows[mutation.index] = mutation.row;
    } else if (mutation._tag === "Move") {
      const movedRows = rows.splice(mutation.toIndex, 1);
      const movedKeys = keys.splice(mutation.toIndex, 1);
      rows.splice(mutation.fromIndex, 0, ...movedRows);
      keys.splice(mutation.fromIndex, 0, ...movedKeys);
      reindexKeys(keys, keyIndexes, Math.min(mutation.fromIndex, mutation.toIndex));
    } else {
      rows.splice(mutation.index, 0, mutation.row);
      keys.splice(mutation.index, 0, mutation.key);
      reindexKeys(keys, keyIndexes, mutation.index);
    }
  }
};

const staleDeltaState = <Row>(state: ClientState<Row>): ClientState<Row> => ({
  ...state,
  status: "stale",
  statusCode: "SnapshotStale",
  message: "Received an invalid delta; waiting for a fresh snapshot.",
});

const staleSnapshotState = <Row>(state: ClientState<Row>): ClientState<Row> => ({
  ...state,
  status: "stale",
  statusCode: "SnapshotStale",
  message: "Received an invalid snapshot; waiting for a fresh snapshot.",
});

const canApplyDeltaFromVersion = <Row>(
  state: ClientState<Row>,
  event: DeltaEvent<Row>,
): boolean => {
  if (state.status !== "ready") {
    return false;
  }
  return (
    isNonNegativeSafeInteger(state.version) &&
    isNonNegativeSafeInteger(event.fromVersion) &&
    isNonNegativeSafeInteger(event.toVersion) &&
    isNonNegativeSafeInteger(event.totalRows) &&
    state.version === event.fromVersion &&
    event.toVersion > state.version
  );
};

export type ClientStateChange =
  | {
      readonly _tag: "All";
    }
  | {
      readonly _tag: "Range";
      readonly start: number;
      readonly end: number;
    }
  | {
      readonly _tag: "None";
    };

export type IncrementalClientStateResult<Row> = {
  readonly current: ClientState<Row>;
  readonly change: ClientStateChange;
};

export type IncrementalClientState<Row> = {
  readonly apply: (event: ViewServerLiveEvent<Row>) => IncrementalClientStateResult<Row>;
};

export const makeIncrementalClientState = <Row>(
  initial: ClientState<Row> = initialClientState<Row>(),
): IncrementalClientState<Row> => {
  const rows = initial.rows.slice();
  const keys = initial.keys.slice();
  const keyIndexes = new Map(keys.map((key, index) => [key, index]));
  let current: ClientState<Row> = { ...initial, rows, keys };

  const staleDelta = (): IncrementalClientStateResult<Row> => {
    current = staleDeltaState(current);
    return { current, change: { _tag: "None" } };
  };

  const apply = (event: ViewServerLiveEvent<Row>): IncrementalClientStateResult<Row> => {
    if (event.type === "snapshot") {
      if (!isValidSnapshotEvent(event)) {
        current = staleSnapshotState(current);
        return { current, change: { _tag: "None" } };
      }
      rows.length = 0;
      keys.length = 0;
      for (let index = 0; index < event.rows.length; index += 1) {
        rows.push(event.rows[index]!);
        keys.push(event.keys[index]!);
      }
      keyIndexes.clear();
      reindexKeys(keys, keyIndexes, 0);
      current = {
        rows,
        keys,
        totalRows: event.totalRows,
        version: event.version,
        status: "ready",
        statusCode: "Ready",
      };
      return { current, change: { _tag: "All" } };
    }
    if (event.type !== "delta") {
      if (event.status === "closed") {
        rows.length = 0;
        keys.length = 0;
        keyIndexes.clear();
        current = initialClientState<Row>();
      }
      current = {
        ...current,
        status: event.status,
        statusCode: event.code,
        message: event.message,
      };
      return { current, change: { _tag: "None" } };
    }
    if (!canApplyDeltaFromVersion(current, event)) {
      return staleDelta();
    }

    const mutations: Array<ClientStateMutation<Row>> = [];
    let start = Number.POSITIVE_INFINITY;
    let end = -1;
    let throughEnd = false;
    for (const operation of event.operations) {
      if (operation.type === "insert") {
        if (!isInsertIndex(operation.index, keys.length) || keyIndexes.has(operation.key)) {
          rollbackMutations(rows, keys, keyIndexes, mutations);
          return staleDelta();
        }
        rows.splice(operation.index, 0, operation.row);
        keys.splice(operation.index, 0, operation.key);
        reindexKeys(keys, keyIndexes, operation.index);
        mutations.push({ _tag: "Insert", index: operation.index, key: operation.key });
        start = Math.min(start, operation.index);
        throughEnd = true;
      } else if (operation.type === "update") {
        if (
          !isExistingIndex(operation.index, keys.length) ||
          keys[operation.index] !== operation.key
        ) {
          rollbackMutations(rows, keys, keyIndexes, mutations);
          return staleDelta();
        }
        mutations.push({ _tag: "Update", index: operation.index, row: rows[operation.index]! });
        rows[operation.index] = operation.row;
        start = Math.min(start, operation.index);
        end = Math.max(end, operation.index);
      } else if (operation.type === "move") {
        if (
          !isExistingIndex(operation.fromIndex, keys.length) ||
          !isExistingIndex(operation.toIndex, keys.length) ||
          keys[operation.fromIndex] !== operation.key
        ) {
          rollbackMutations(rows, keys, keyIndexes, mutations);
          return staleDelta();
        }
        const movedRows = rows.splice(operation.fromIndex, 1);
        const movedKeys = keys.splice(operation.fromIndex, 1);
        rows.splice(operation.toIndex, 0, ...movedRows);
        keys.splice(operation.toIndex, 0, ...movedKeys);
        reindexKeys(keys, keyIndexes, Math.min(operation.fromIndex, operation.toIndex));
        mutations.push({
          _tag: "Move",
          fromIndex: operation.fromIndex,
          toIndex: operation.toIndex,
        });
        start = Math.min(start, operation.fromIndex, operation.toIndex);
        end = Math.max(end, operation.fromIndex, operation.toIndex);
      } else {
        const index = keyIndexes.get(operation.key);
        if (index === undefined) {
          rollbackMutations(rows, keys, keyIndexes, mutations);
          return staleDelta();
        }
        const removedRows = rows.splice(index, 1);
        keys.splice(index, 1);
        keyIndexes.delete(operation.key);
        reindexKeys(keys, keyIndexes, index);
        mutations.push({
          _tag: "Remove",
          index,
          key: operation.key,
          row: removedRows[0]!,
        });
        start = Math.min(start, index);
        throughEnd = true;
      }
    }
    if (event.totalRows < rows.length) {
      rollbackMutations(rows, keys, keyIndexes, mutations);
      return staleDelta();
    }
    current = {
      rows,
      keys,
      totalRows: event.totalRows,
      version: event.toVersion,
      status: "ready",
      statusCode: "Ready",
    };
    const change: ClientStateChange = Number.isFinite(start)
      ? {
          _tag: "Range",
          start,
          end: throughEnd ? rows.length - 1 : Math.min(end, rows.length - 1),
        }
      : { _tag: "None" };
    return { current, change };
  };

  return { apply };
};

export const applyEvent = <Row>(
  state: ClientState<Row>,
  event: ViewServerLiveEvent<Row>,
): ClientState<Row> => makeIncrementalClientState(state).apply(event).current;
