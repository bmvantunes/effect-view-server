import { make as makeBigDecimal } from "effect/BigDecimal";
import { isWireSafeBigDecimal } from "./wire-safe-big-decimal";

type QueryRecord = Readonly<Record<string, unknown>>;

const ownedQuerySnapshots = new WeakSet<object>();

export const viewServerQuerySnapshotErrorMessage =
  "Query input could not be snapshotted at subscribe.";

const isPlainRecord = (value: unknown): value is QueryRecord =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  !isWireSafeBigDecimal(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

const ownEntries = (value: QueryRecord): ReadonlyArray<readonly [string, unknown]> => {
  if (Object.getOwnPropertySymbols(value).length > 0) {
    throw new TypeError("Query input must not contain symbol properties.");
  }
  const entries: Array<readonly [string, unknown]> = [];
  for (const key of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      throw new TypeError("Query input fields must be own enumerable data properties.");
    }
    entries.push([key, descriptor.value]);
  }
  return entries;
};

const arrayValues = (value: ReadonlyArray<unknown>): ReadonlyArray<unknown> => {
  if (
    Object.getPrototypeOf(value) !== Array.prototype ||
    Object.getOwnPropertySymbols(value).length > 0
  ) {
    throw new TypeError("Query input arrays must be plain arrays.");
  }
  // Array.isArray plus the exact Array prototype guarantees the non-configurable data descriptor.
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length")!;
  const length: number = lengthDescriptor.value;
  const values: Array<unknown> = [];
  const allowed = new Set(["length"]);
  for (let index = 0; index < length; index += 1) {
    const key = String(index);
    allowed.add(key);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      throw new TypeError("Query input arrays must be dense data arrays.");
    }
    values.push(descriptor.value);
  }
  if (Object.getOwnPropertyNames(value).some((key) => !allowed.has(key))) {
    throw new TypeError("Query input arrays must not contain extra properties.");
  }
  return values;
};

type SnapshotFrame =
  | { readonly _tag: "enter"; readonly value: unknown }
  | {
      readonly _tag: "exitArray";
      readonly source: ReadonlyArray<unknown>;
      readonly count: number;
    }
  | {
      readonly _tag: "exitRecord";
      readonly source: QueryRecord;
      readonly keys: ReadonlyArray<string>;
    };

const snapshotQueryValue = (input: unknown): unknown => {
  const frames: Array<SnapshotFrame> = [{ _tag: "enter", value: input }];
  const results: Array<unknown> = [];
  const active = new WeakSet<object>();
  const completed = new WeakMap<object, unknown>();
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exitArray") {
      const values = results.splice(results.length - frame.count, frame.count);
      const snapshot = Object.freeze(values);
      active.delete(frame.source);
      completed.set(frame.source, snapshot);
      results.push(snapshot);
      continue;
    }
    if (frame._tag === "exitRecord") {
      const values = results.splice(results.length - frame.keys.length, frame.keys.length);
      const snapshot: Record<string, unknown> = {};
      for (const [index, key] of frame.keys.entries()) {
        Object.defineProperty(snapshot, key, {
          configurable: false,
          enumerable: true,
          value: values[index],
          writable: false,
        });
      }
      Object.freeze(snapshot);
      active.delete(frame.source);
      completed.set(frame.source, snapshot);
      results.push(snapshot);
      continue;
    }
    const value = frame.value;
    if (isWireSafeBigDecimal(value)) {
      results.push(makeBigDecimal(value.value, value.scale));
      continue;
    }
    if (typeof value !== "object" || value === null) {
      if (value === undefined || typeof value === "function" || typeof value === "symbol") {
        throw new TypeError("Query input contains an unsupported value.");
      }
      if (typeof value === "number" && !Number.isFinite(value)) {
        throw new TypeError("Query input numbers must be finite.");
      }
      results.push(value);
      continue;
    }
    const cached = completed.get(value);
    if (cached !== undefined) {
      results.push(cached);
      continue;
    }
    if (active.has(value)) {
      throw new TypeError("Query input contains a cycle.");
    }
    active.add(value);
    if (Array.isArray(value)) {
      const values = arrayValues(value);
      frames.push({ _tag: "exitArray", source: value, count: values.length });
      for (let index = values.length - 1; index >= 0; index -= 1) {
        frames.push({ _tag: "enter", value: values[index] });
      }
      continue;
    }
    if (!isPlainRecord(value)) {
      throw new TypeError("Query input contains an unsupported object value.");
    }
    const entries = ownEntries(value);
    frames.push({
      _tag: "exitRecord",
      source: value,
      keys: Object.freeze(entries.map(([key]) => key)),
    });
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      frames.push({ _tag: "enter", value: entries[index]![1] });
    }
  }
  return results[0];
};

export function snapshotViewServerQuery<Query extends object>(query: Query): Query;
export function snapshotViewServerQuery(query: unknown): unknown;
export function snapshotViewServerQuery(query: unknown): unknown {
  if (isPlainRecord(query) && ownedQuerySnapshots.has(query)) {
    return query;
  }
  const snapshot = snapshotQueryValue(query);
  if (!isPlainRecord(snapshot)) {
    throw new TypeError("Query input snapshot must remain a plain object.");
  }
  ownedQuerySnapshots.add(snapshot);
  return snapshot;
}

export const ownViewServerQuerySnapshot = <Query extends QueryRecord>(query: Query): Query => {
  if (!Object.isFrozen(query)) {
    throw new TypeError("Owned query snapshots must be frozen.");
  }
  ownedQuerySnapshots.add(query);
  return query;
};
