import { make as makeBigDecimal } from "effect/BigDecimal";
import { Result, Schema } from "effect";
import {
  hasPlainRecordPrototype,
  inspectArrayData,
  inspectPlainRecordData,
} from "./structural-data";
import { inspectWireSafeBigDecimal } from "./wire-safe-big-decimal";

type QueryRecord = Readonly<Record<string, unknown>>;
type QueryInput = Schema.Schema.Type<typeof Schema.Unknown>;

const ownedQuerySnapshots = new WeakSet<object>();

export const viewServerQuerySnapshotErrorMessage = "Query input could not be snapshotted.";

const isPlainRecord = (value: unknown): value is QueryRecord => hasPlainRecordPrototype(value);

const ownEntries = (value: QueryRecord): ReadonlyArray<readonly [string, unknown]> => {
  const inspection = inspectPlainRecordData(value);
  if (inspection._tag === "Success") {
    return inspection.snapshot.entries;
  }
  if (inspection.reason === "invalidRecord") {
    throw new TypeError("Query input must not contain symbol properties.");
  }
  throw new TypeError("Query input fields must be own enumerable data properties.");
};

const arrayData = (value: ReadonlyArray<unknown>) => {
  const inspection = inspectArrayData(value);
  if (inspection._tag === "Success") {
    return inspection.snapshot;
  }
  if (
    inspection.reason === "invalidArray" ||
    inspection.reason === "invalidReflection" ||
    (inspection.reason === "invalidExtraProperty" && typeof inspection.key === "symbol")
  ) {
    throw new TypeError("Query input arrays must be plain arrays.");
  }
  if (inspection.reason === "invalidEntry") {
    throw new TypeError("Query input arrays must be dense data arrays.");
  }
  throw new TypeError("Query input arrays must contain enumerable data properties.");
};

type SnapshotFrame =
  | { readonly _tag: "enter"; readonly value: unknown }
  | {
      readonly _tag: "exitArray";
      readonly source: ReadonlyArray<unknown>;
      readonly valueCount: number;
      readonly extraKeys: ReadonlyArray<string>;
    }
  | {
      readonly _tag: "exitRecord";
      readonly source: QueryRecord;
      readonly keys: ReadonlyArray<string>;
    };

const snapshotQueryValue = (input: QueryInput): unknown => {
  const frames: Array<SnapshotFrame> = [{ _tag: "enter", value: input }];
  const results: Array<unknown> = [];
  const active = new WeakSet<object>();
  const completed = new WeakMap<object, unknown>();
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exitArray") {
      const count = frame.valueCount + frame.extraKeys.length;
      const values = results.splice(results.length - count, count);
      const extraValues = values.splice(frame.valueCount, frame.extraKeys.length);
      for (const [index, key] of frame.extraKeys.entries()) {
        Object.defineProperty(values, key, {
          configurable: false,
          enumerable: true,
          value: extraValues[index],
          writable: false,
        });
      }
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
    if (typeof value === "object" && value !== null) {
      const cached = completed.get(value);
      if (cached !== undefined) {
        results.push(cached);
        continue;
      }
    }
    const decimal = inspectWireSafeBigDecimal(value);
    if (decimal._tag === "Success") {
      const snapshot = makeBigDecimal(decimal.coefficient, decimal.scale);
      completed.set(decimal.source, snapshot);
      results.push(snapshot);
      continue;
    }
    if (decimal._tag === "UnsafeBigDecimal" || decimal._tag === "ReflectionFailure") {
      throw new TypeError("Query input contains an unsupported object value.");
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
    if (active.has(value)) {
      throw new TypeError("Query input contains a cycle.");
    }
    active.add(value);
    if (Array.isArray(value)) {
      const snapshot = arrayData(value);
      frames.push({
        _tag: "exitArray",
        source: value,
        valueCount: snapshot.values.length,
        extraKeys: snapshot.extraEntries.map(([key]) => key),
      });
      for (let index = snapshot.extraEntries.length - 1; index >= 0; index -= 1) {
        frames.push({ _tag: "enter", value: snapshot.extraEntries[index]![1] });
      }
      for (let index = snapshot.values.length - 1; index >= 0; index -= 1) {
        frames.push({ _tag: "enter", value: snapshot.values[index] });
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
export function snapshotViewServerQuery(query: QueryInput): Readonly<Record<string, unknown>>;
export function snapshotViewServerQuery(query: QueryInput): unknown {
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

export type CapturedSourceHealthInput<Topic extends string = string> = {
  readonly topic: Topic;
  readonly route: ReadonlyArray<Readonly<Record<string, unknown>>>;
};

/**
 * Captures an owned Source Health request without invoking input accessors.
 *
 * The typed overload preserves the caller's already-validated public topic
 * type; this utility proves only that the captured runtime value is a string.
 * Runtime/client adapters must still validate configured Source ownership
 * before using the result.
 */
export function captureSourceHealthInput<const Topic extends string>(
  input: QueryRecord & { readonly topic: Topic },
): Result.Result<CapturedSourceHealthInput<Topic>, unknown>;
export function captureSourceHealthInput(
  input: QueryInput,
): Result.Result<CapturedSourceHealthInput<string>, unknown>;
export function captureSourceHealthInput(
  input: QueryInput,
): Result.Result<CapturedSourceHealthInput<string>, unknown> {
  return Result.try(() => {
    const captured = snapshotViewServerQuery(input);
    const keys = Reflect.ownKeys(captured);
    const hasRoute = Object.hasOwn(captured, "routeBy");
    const expectedKeys = hasRoute ? ["topic", "routeBy"] : ["topic"];
    const route = captured["routeBy"];
    const topic = captured["topic"];
    if (
      keys.length !== expectedKeys.length ||
      keys.some((key) => !expectedKeys.some((expected) => expected === key)) ||
      typeof topic !== "string" ||
      (hasRoute && !isPlainRecord(route))
    ) {
      throw new TypeError(
        "Source Health input must contain topic and leased routeBy only when required.",
      );
    }
    return {
      topic,
      route: isPlainRecord(route) ? [route] : [],
    };
  });
}
