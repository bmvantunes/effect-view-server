import type { RowSchema } from "@effect-view-server/config";
import { isRawQueryFilterOperatorKey } from "@effect-view-server/config/internal";
import { makeSchemaJsonIdentity, type SchemaJsonIdentity } from "@effect-view-server/effect-utils";
import { Result, Schema } from "effect";

type QueryRecord = Readonly<Record<string, unknown>>;
type RowFieldSchema = Schema.Codec<unknown, unknown, never, never>;

const isRowFieldSchema = (value: unknown): value is RowFieldSchema => Schema.isSchema(value);

const isPlainRecord = (value: unknown): value is QueryRecord => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const defineSnapshotProperty = (
  target: Record<string, unknown>,
  key: string,
  value: unknown,
): void => {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const ownEnumerableDataProperties = (
  value: QueryRecord,
): ReadonlyArray<readonly [string, unknown]> => {
  const entries: Array<readonly [string, unknown]> = [];
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) {
      throw new TypeError("Leased gRPC query fields could not be inspected.");
    }
    if (!descriptor.enumerable) {
      continue;
    }
    if (typeof key !== "string" || !("value" in descriptor)) {
      throw new TypeError("Leased gRPC query fields must be own data properties.");
    }
    entries.push([key, descriptor.value]);
  }
  return entries;
};

const snapshotArrayValues = (
  value: ReadonlyArray<unknown>,
  snapshotEntry: (entry: unknown) => unknown,
): ReadonlyArray<unknown> => {
  const snapshot: Array<unknown> = [];
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length")!;
  const length: number = lengthDescriptor.value;
  for (let index = 0; index < length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (descriptor === undefined || !("value" in descriptor)) {
      throw new TypeError("Leased gRPC query array entries must be own data properties.");
    }
    snapshot.push(snapshotEntry(descriptor.value));
  }
  return snapshot;
};

const snapshotSchemaValueInput = (value: unknown, active: WeakSet<object>): unknown => {
  if (Array.isArray(value)) {
    if (active.has(value)) {
      throw new TypeError("Leased gRPC query contains a cycle.");
    }
    active.add(value);
    const snapshot = snapshotArrayValues(value, (entry) => snapshotSchemaValueInput(entry, active));
    active.delete(value);
    return snapshot;
  }
  if (isPlainRecord(value)) {
    if (active.has(value)) {
      throw new TypeError("Leased gRPC query contains a cycle.");
    }
    active.add(value);
    const snapshot: Record<string, unknown> = {};
    for (const [key, entry] of ownEnumerableDataProperties(value)) {
      defineSnapshotProperty(snapshot, key, snapshotSchemaValueInput(entry, active));
    }
    active.delete(value);
    return snapshot;
  }
  return value;
};

const snapshotQueryValue = (value: unknown, active: WeakSet<object>): unknown => {
  if (Array.isArray(value)) {
    if (active.has(value)) {
      throw new TypeError("Leased gRPC query contains a cycle.");
    }
    active.add(value);
    const snapshot = snapshotArrayValues(value, (entry) => snapshotQueryValue(entry, active));
    active.delete(value);
    return snapshot;
  }
  if (isPlainRecord(value)) {
    if (active.has(value)) {
      throw new TypeError("Leased gRPC query contains a cycle.");
    }
    active.add(value);
    const snapshot: Record<string, unknown> = {};
    for (const [key, entry] of ownEnumerableDataProperties(value)) {
      defineSnapshotProperty(snapshot, key, snapshotQueryValue(entry, active));
    }
    active.delete(value);
    return snapshot;
  }
  if (typeof value === "object" && value !== null) {
    return structuredClone(value);
  }
  return value;
};

const snapshotWithFieldSchema = (
  identity: SchemaJsonIdentity,
  value: unknown,
  active: WeakSet<object>,
): unknown => {
  const owned = snapshotSchemaValueInput(value, active);
  const materialized = Result.try(() => identity.materializeDecoded(owned));
  return Result.isSuccess(materialized) ? materialized.success : snapshotQueryValue(owned, active);
};

const snapshotOperatorFilter = (
  identity: SchemaJsonIdentity,
  entries: ReadonlyArray<readonly [string, unknown]>,
  active: WeakSet<object>,
): Record<string, unknown> => {
  const snapshot: Record<string, unknown> = {};
  for (const [operator, value] of entries) {
    if (operator === "in" && Array.isArray(value)) {
      defineSnapshotProperty(
        snapshot,
        operator,
        snapshotArrayValues(value, (entry) => snapshotWithFieldSchema(identity, entry, active)),
      );
    } else if (operator === "startsWith" || !isRawQueryFilterOperatorKey(operator)) {
      defineSnapshotProperty(snapshot, operator, snapshotQueryValue(value, active));
    } else {
      defineSnapshotProperty(snapshot, operator, snapshotWithFieldSchema(identity, value, active));
    }
  }
  return snapshot;
};

const snapshotWhereFilter = (
  fieldSchema: RowFieldSchema,
  filter: unknown,
  active: WeakSet<object>,
): unknown => {
  const identity = makeSchemaJsonIdentity(fieldSchema);
  const owned = snapshotSchemaValueInput(filter, active);
  const literal = Result.try(() => identity.materializeDecoded(owned));
  if (Result.isSuccess(literal)) {
    return literal.success;
  }
  if (isPlainRecord(owned)) {
    const entries = ownEnumerableDataProperties(owned);
    if (entries.some(([operator]) => isRawQueryFilterOperatorKey(operator))) {
      return snapshotOperatorFilter(identity, entries, active);
    }
  }
  return snapshotQueryValue(owned, active);
};

const snapshotWhere = (schema: RowSchema, where: unknown, active: WeakSet<object>): unknown => {
  if (!isPlainRecord(where)) {
    return snapshotQueryValue(where, active);
  }
  const snapshot: Record<string, unknown> = {};
  for (const [field, filter] of ownEnumerableDataProperties(where)) {
    const fieldSchema = schema.fields[field];
    defineSnapshotProperty(
      snapshot,
      field,
      !isRowFieldSchema(fieldSchema)
        ? snapshotQueryValue(filter, active)
        : snapshotWhereFilter(fieldSchema, filter, active),
    );
  }
  return snapshot;
};

export function snapshotLeasedGrpcQuery<Query extends QueryRecord>(
  schema: RowSchema,
  query: Query,
): Query;
export function snapshotLeasedGrpcQuery(schema: RowSchema, query: QueryRecord): QueryRecord {
  const active = new WeakSet<object>();
  const snapshot: Record<string, unknown> = {};
  for (const [key, value] of ownEnumerableDataProperties(query)) {
    defineSnapshotProperty(
      snapshot,
      key,
      key === "where" ? snapshotWhere(schema, value, active) : snapshotQueryValue(value, active),
    );
  }
  return snapshot;
}
