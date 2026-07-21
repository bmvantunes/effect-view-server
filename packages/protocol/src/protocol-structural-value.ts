import {
  hasPlainRecordPrototype,
  inspectDenseArrayData,
  inspectPlainRecordData,
  type PlainRecordSnapshot,
} from "@effect-view-server/effect-utils";

export const isProtocolPlainRecord = hasPlainRecordPrototype;

export type ProtocolRecordSnapshot = PlainRecordSnapshot;

export const protocolRecordSnapshot = (value: unknown): ProtocolRecordSnapshot | undefined => {
  const inspection = inspectPlainRecordData(value);
  return inspection._tag === "Success" ? inspection.snapshot : undefined;
};

export const protocolSnapshotDataValue = (
  snapshot: ProtocolRecordSnapshot,
  key: string,
): unknown => {
  for (const [entryKey, entryValue] of snapshot.entries) {
    if (entryKey === key) {
      return entryValue;
    }
  }
  return undefined;
};

export const protocolRecordDataEntries = (
  value: Readonly<Record<string, unknown>>,
): ReadonlyArray<readonly [string, unknown]> | undefined => {
  return protocolRecordSnapshot(value)?.entries;
};

export const protocolSnapshotHasExactDataKeys = (
  snapshot: ProtocolRecordSnapshot,
  expected: ReadonlySet<string>,
): boolean => {
  return (
    snapshot.entries.length === expected.size &&
    snapshot.entries.every(([key]) => expected.has(key))
  );
};

export const protocolDenseArray = (value: unknown): ReadonlyArray<unknown> | undefined => {
  const inspection = inspectDenseArrayData(value);
  return inspection._tag === "Success" ? inspection.values : undefined;
};
