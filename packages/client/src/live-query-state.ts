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

type StructuralBatch<Row> =
  | { readonly _tag: "NotStaged" }
  | { readonly _tag: "Invalid" }
  | {
      readonly _tag: "Staged";
      readonly rows: ReadonlyArray<Row>;
      readonly keys: ReadonlyArray<string>;
      readonly start: number;
      readonly end: number;
    };

type SequenceNode<Row> = {
  readonly id: number;
  readonly priority: bigint;
  readonly key: string;
  row: Row;
  left: SequenceNode<Row> | undefined;
  right: SequenceNode<Row> | undefined;
  parent: SequenceNode<Row> | undefined;
  size: number;
};

const sequenceSize = <Row>(node: SequenceNode<Row> | undefined): number => node?.size ?? 0;

const updateSequenceNode = <Row>(node: SequenceNode<Row>): void => {
  node.size = sequenceSize(node.left) + sequenceSize(node.right) + 1;
  if (node.left !== undefined) {
    node.left.parent = node;
  }
  if (node.right !== undefined) {
    node.right.parent = node;
  }
};

const sequencePrecedes = <Row>(left: SequenceNode<Row>, right: SequenceNode<Row>): boolean =>
  left.priority < right.priority;

const mergeSequence = <Row>(
  left: SequenceNode<Row> | undefined,
  right: SequenceNode<Row> | undefined,
): SequenceNode<Row> | undefined => {
  if (left === undefined) {
    if (right !== undefined) {
      right.parent = undefined;
    }
    return right;
  }
  if (right === undefined) {
    left.parent = undefined;
    return left;
  }
  if (sequencePrecedes(left, right)) {
    left.right = mergeSequence(left.right, right);
    updateSequenceNode(left);
    left.parent = undefined;
    return left;
  }
  right.left = mergeSequence(left, right.left);
  updateSequenceNode(right);
  right.parent = undefined;
  return right;
};

const splitSequence = <Row>(
  root: SequenceNode<Row> | undefined,
  count: number,
): readonly [SequenceNode<Row> | undefined, SequenceNode<Row> | undefined] => {
  if (root === undefined) {
    return [undefined, undefined];
  }
  const leftSize = sequenceSize(root.left);
  if (count <= leftSize) {
    const [left, right] = splitSequence(root.left, count);
    root.left = right;
    updateSequenceNode(root);
    root.parent = undefined;
    if (left !== undefined) {
      left.parent = undefined;
    }
    return [left, root];
  }
  const [left, right] = splitSequence(root.right, count - leftSize - 1);
  root.right = left;
  updateSequenceNode(root);
  root.parent = undefined;
  if (right !== undefined) {
    right.parent = undefined;
  }
  return [root, right];
};

const sequenceNodeAt = <Row>(
  root: SequenceNode<Row> | undefined,
  index: number,
): SequenceNode<Row> | undefined => {
  let current = root;
  let remaining = index;
  while (current !== undefined) {
    const leftSize = sequenceSize(current.left);
    if (remaining === leftSize) {
      return current;
    }
    if (remaining < leftSize) {
      current = current.left;
    } else {
      remaining -= leftSize + 1;
      current = current.right;
    }
  }
  return undefined;
};

const sequenceNodeIndex = <Row>(node: SequenceNode<Row>): number => {
  let index = sequenceSize(node.left);
  let current = node;
  while (current.parent !== undefined) {
    if (current.parent.right === current) {
      index += sequenceSize(current.parent.left) + 1;
    }
    current = current.parent;
  }
  return index;
};

const sequencePriority = (id: number): bigint => {
  let value = id + 0x9e3779b9;
  value = Math.imul(value ^ (value >>> 16), 0x21f0aaad);
  value = Math.imul(value ^ (value >>> 15), 0x735a2d97);
  return (BigInt((value ^ (value >>> 15)) >>> 0) << 53n) | BigInt(id);
};

const makeSequenceNode = <Row>(id: number, key: string, row: Row): SequenceNode<Row> => ({
  id,
  priority: sequencePriority(id),
  key,
  row,
  left: undefined,
  right: undefined,
  parent: undefined,
  size: 1,
});

const materializeSequence = <Row>(
  root: SequenceNode<Row> | undefined,
): { readonly rows: ReadonlyArray<Row>; readonly keys: ReadonlyArray<string> } => {
  const rows: Array<Row> = [];
  const keys: Array<string> = [];
  const stack: Array<SequenceNode<Row>> = [];
  let current = root;
  while (current !== undefined || stack.length > 0) {
    while (current !== undefined) {
      stack.push(current);
      current = current.left;
    }
    const node = stack.pop()!;
    rows.push(node.row);
    keys.push(node.key);
    current = node.right;
  }
  return { rows, keys };
};

const shouldStageStructuralBatch = <Row>(
  rowCount: number,
  operations: DeltaEvent<Row>["operations"],
): boolean => {
  let structuralCount = 0;
  for (const operation of operations) {
    if (operation.type !== "update") {
      structuralCount += 1;
    }
  }
  // Below this benchmark-derived density, the in-place path avoids materializing
  // the whole viewport and preserves cheap, narrow changes such as a tail replace.
  return structuralCount >= 64 && structuralCount * 4 >= Math.max(rowCount, 1);
};

const stageStructuralBatch = <Row>(
  rows: ReadonlyArray<Row>,
  keys: ReadonlyArray<string>,
  operations: DeltaEvent<Row>["operations"],
): StructuralBatch<Row> => {
  if (!shouldStageStructuralBatch(rows.length, operations)) {
    return { _tag: "NotStaged" };
  }

  const nodes = new Map<string, SequenceNode<Row>>();
  let root: SequenceNode<Row> | undefined;
  let nextId = 0;
  for (let index = 0; index < keys.length; index += 1) {
    const node = makeSequenceNode(++nextId, keys[index]!, rows[index]!);
    nodes.set(node.key, node);
    root = mergeSequence(root, node);
  }

  let start = Number.POSITIVE_INFINITY;
  let end = -1;
  let throughEnd = false;
  for (const operation of operations) {
    const length = sequenceSize(root);
    if (operation.type === "insert") {
      if (!isInsertIndex(operation.index, length) || nodes.has(operation.key)) {
        return { _tag: "Invalid" };
      }
      const [left, right] = splitSequence(root, operation.index);
      const node = makeSequenceNode(++nextId, operation.key, operation.row);
      nodes.set(operation.key, node);
      root = mergeSequence(mergeSequence(left, node), right);
      start = Math.min(start, operation.index);
      throughEnd = true;
    } else if (operation.type === "update") {
      const node = sequenceNodeAt(root, operation.index);
      if (node === undefined || node.key !== operation.key) {
        return { _tag: "Invalid" };
      }
      node.row = operation.row;
      start = Math.min(start, operation.index);
      end = Math.max(end, operation.index);
    } else if (operation.type === "move") {
      const node = sequenceNodeAt(root, operation.fromIndex);
      if (
        node === undefined ||
        node.key !== operation.key ||
        !isExistingIndex(operation.toIndex, length)
      ) {
        return { _tag: "Invalid" };
      }
      const [before, from] = splitSequence(root, operation.fromIndex);
      const [moved, after] = splitSequence(from, 1);
      root = mergeSequence(before, after);
      const [left, right] = splitSequence(root, operation.toIndex);
      root = mergeSequence(mergeSequence(left, moved), right);
      start = Math.min(start, operation.fromIndex, operation.toIndex);
      end = Math.max(end, operation.fromIndex, operation.toIndex);
    } else {
      const node = nodes.get(operation.key);
      if (node === undefined) {
        return { _tag: "Invalid" };
      }
      const index = sequenceNodeIndex(node);
      const [before, from] = splitSequence(root, index);
      const [, after] = splitSequence(from, 1);
      root = mergeSequence(before, after);
      nodes.delete(operation.key);
      start = Math.min(start, index);
      throughEnd = true;
    }
  }
  const materialized = materializeSequence(root);
  return {
    _tag: "Staged",
    rows: materialized.rows,
    keys: materialized.keys,
    start,
    end: throughEnd ? materialized.rows.length - 1 : Math.min(end, materialized.rows.length - 1),
  };
};

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
  /**
   * Applies an event in place. `current.rows` and `current.keys` are stable,
   * controller-owned arrays that may be mutated by later calls to `apply`.
   * Consumers that retain historical states must copy them at that boundary.
   */
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
        current = { ...initialClientState<Row>(), rows, keys };
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

    const replacement = stageStructuralBatch(rows, keys, event.operations);
    if (
      replacement._tag === "Invalid" ||
      (replacement._tag === "Staged" && event.totalRows < replacement.rows.length)
    ) {
      return staleDelta();
    }
    if (replacement._tag === "Staged") {
      rows.length = 0;
      keys.length = 0;
      for (let index = 0; index < replacement.rows.length; index += 1) {
        rows.push(replacement.rows[index]!);
        keys.push(replacement.keys[index]!);
      }
      keyIndexes.clear();
      reindexKeys(keys, keyIndexes, 0);
      current = {
        rows,
        keys,
        totalRows: event.totalRows,
        version: event.toVersion,
        status: "ready",
        statusCode: "Ready",
      };
      return {
        current,
        change: {
          _tag: "Range",
          start: replacement.start,
          end: replacement.end,
        },
      };
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
): ClientState<Row> => {
  if (event.type !== "snapshot" && event.type !== "delta") {
    const current = event.status === "closed" ? initialClientState<Row>() : state;
    return {
      ...current,
      status: event.status,
      statusCode: event.code,
      message: event.message,
    };
  }
  return makeIncrementalClientState(state).apply(event).current;
};
