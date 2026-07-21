import {
  hasPlainRecordPrototype,
  inspectDenseArrayData,
  inspectPlainRecordData,
  type PlainRecordSnapshot,
} from "@effect-view-server/effect-utils";

export { hasPlainRecordPrototype, type PlainRecordSnapshot };

export const plainRecordSnapshot = (
  value: unknown,
  invalidRecord: () => never,
  invalidProperty: () => never,
): PlainRecordSnapshot => {
  const inspection = inspectPlainRecordData(value);
  if (inspection._tag === "Success") {
    return inspection.snapshot;
  }
  return inspection.reason === "invalidRecord" ? invalidRecord() : invalidProperty();
};

export const denseArrayValues = (
  value: unknown,
  invalidArray: () => never,
  invalidEntry: () => never,
  invalidExtraProperty: () => never,
): ReadonlyArray<unknown> => {
  const inspection = inspectDenseArrayData(value);
  if (inspection._tag === "Success") {
    return inspection.values;
  }
  if (inspection.reason === "invalidArray" || inspection.reason === "invalidReflection") {
    return invalidArray();
  }
  return inspection.reason === "invalidEntry" ? invalidEntry() : invalidExtraProperty();
};
