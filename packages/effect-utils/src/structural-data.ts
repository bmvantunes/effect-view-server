export type PlainRecordSnapshot = {
  readonly source: Readonly<Record<string, unknown>>;
  readonly entries: ReadonlyArray<readonly [string, unknown]>;
};

export type PlainRecordDataInspection =
  | { readonly _tag: "Data"; readonly value: unknown }
  | { readonly _tag: "Missing" }
  | { readonly _tag: "InvalidProperty" }
  | { readonly _tag: "ReflectionFailure" };

export type PlainRecordShapeSnapshot = {
  readonly source: Readonly<Record<string, unknown>>;
  readonly stringKeys: ReadonlyArray<string>;
  readonly symbolKeys: ReadonlyArray<symbol>;
  readonly inspectData: (key: string) => PlainRecordDataInspection;
};

export type PlainRecordShapeInspection =
  | { readonly _tag: "Success"; readonly snapshot: PlainRecordShapeSnapshot }
  | { readonly _tag: "Failure"; readonly reason: "invalidRecord" | "invalidReflection" };

export type PlainRecordInspection =
  | { readonly _tag: "Success"; readonly snapshot: PlainRecordSnapshot }
  | { readonly _tag: "Failure"; readonly reason: "invalidRecord" | "invalidProperty" };

export type DenseArrayInspection =
  | { readonly _tag: "Success"; readonly values: ReadonlyArray<unknown> }
  | ArrayDataFailure;

export type ArrayDataFailure =
  | { readonly _tag: "Failure"; readonly reason: "invalidArray" }
  | { readonly _tag: "Failure"; readonly reason: "invalidEntry" }
  | { readonly _tag: "Failure"; readonly reason: "invalidReflection" }
  | {
      readonly _tag: "Failure";
      readonly reason: "invalidExtraProperty";
      readonly key: PropertyKey;
    };

export type ArrayDataSnapshot = {
  readonly source: ReadonlyArray<unknown>;
  readonly values: ReadonlyArray<unknown>;
  readonly extraEntries: ReadonlyArray<readonly [string, unknown]>;
};

export type ArrayDataInspection =
  | { readonly _tag: "Success"; readonly snapshot: ArrayDataSnapshot }
  | ArrayDataFailure;

const enumerableDataDescriptor = (
  value: object,
  key: PropertyKey,
): PropertyDescriptor | undefined => {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && descriptor.enumerable && "value" in descriptor
    ? descriptor
    : undefined;
};

const isRecordCandidate = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const hasPlainRecordPrototype = (
  value: unknown,
): value is Readonly<Record<string, unknown>> => {
  try {
    return (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value) &&
      Object.getPrototypeOf(value) === Object.prototype
    );
  } catch {
    return false;
  }
};

export const inspectPlainRecordShape = (value: unknown): PlainRecordShapeInspection => {
  try {
    if (!isRecordCandidate(value) || Object.getPrototypeOf(value) !== Object.prototype) {
      return { _tag: "Failure", reason: "invalidRecord" };
    }
    const stringKeys: Array<string> = [];
    const symbolKeys: Array<symbol> = [];
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key === "string") {
        stringKeys.push(key);
      } else {
        symbolKeys.push(key);
      }
    }
    const dataByKey = new Map<string, PlainRecordDataInspection>();
    const inspectData = (key: string): PlainRecordDataInspection => {
      const cached = dataByKey.get(key);
      if (cached !== undefined) {
        return cached;
      }
      let inspected: PlainRecordDataInspection;
      try {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        inspected =
          descriptor === undefined
            ? { _tag: "Missing" }
            : !descriptor.enumerable || !("value" in descriptor)
              ? { _tag: "InvalidProperty" }
              : { _tag: "Data", value: descriptor.value };
      } catch {
        inspected = { _tag: "ReflectionFailure" };
      }
      dataByKey.set(key, inspected);
      return inspected;
    };
    return {
      _tag: "Success",
      snapshot: Object.freeze({
        source: value,
        stringKeys: Object.freeze(stringKeys),
        symbolKeys: Object.freeze(symbolKeys),
        inspectData,
      }),
    };
  } catch {
    return { _tag: "Failure", reason: "invalidReflection" };
  }
};

export const inspectPlainRecordData = (value: unknown): PlainRecordInspection => {
  const shape = inspectPlainRecordShape(value);
  if (shape._tag === "Failure" || shape.snapshot.symbolKeys.length > 0) {
    return { _tag: "Failure", reason: "invalidRecord" };
  }
  const entries: Array<readonly [string, unknown]> = [];
  for (const key of shape.snapshot.stringKeys) {
    const inspected = shape.snapshot.inspectData(key);
    if (inspected._tag === "ReflectionFailure") {
      return { _tag: "Failure", reason: "invalidRecord" };
    }
    if (inspected._tag !== "Data") {
      return { _tag: "Failure", reason: "invalidProperty" };
    }
    entries.push([key, inspected.value]);
  }
  return { _tag: "Success", snapshot: { source: shape.snapshot.source, entries } };
};

export const inspectArrayData = (value: unknown): ArrayDataInspection => {
  try {
    if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype) {
      return { _tag: "Failure", reason: "invalidArray" };
    }
    const keys = Reflect.ownKeys(value);
    const stringKeys: Array<string> = [];
    for (const key of keys) {
      if (typeof key !== "string") {
        return { _tag: "Failure", reason: "invalidExtraProperty", key };
      }
      stringKeys.push(key);
    }
    // Exact built-in arrays guarantee a non-configurable numeric length data property.
    const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length")!;
    const length = lengthDescriptor.value;
    if (typeof length !== "number" || !Number.isSafeInteger(length) || length < 0) {
      return { _tag: "Failure", reason: "invalidArray" };
    }
    if (stringKeys.length < length + 1) {
      return { _tag: "Failure", reason: "invalidEntry" };
    }
    const keySet = new Set(stringKeys);
    const elementKeys = new Set<string>();
    for (let index = 0; index < length; index += 1) {
      const key = String(index);
      elementKeys.add(key);
      if (!keySet.has(key)) {
        return { _tag: "Failure", reason: "invalidEntry" };
      }
    }
    const values: Array<unknown> = [];
    for (let index = 0; index < length; index += 1) {
      const descriptor = enumerableDataDescriptor(value, String(index));
      if (descriptor === undefined) {
        return { _tag: "Failure", reason: "invalidEntry" };
      }
      values.push(descriptor.value);
    }
    const extraEntries: Array<readonly [string, unknown]> = [];
    for (const key of stringKeys) {
      if (key === "length" || elementKeys.has(key)) {
        continue;
      }
      const descriptor = enumerableDataDescriptor(value, key);
      if (descriptor === undefined) {
        return { _tag: "Failure", reason: "invalidExtraProperty", key };
      }
      extraEntries.push([key, descriptor.value]);
    }
    return { _tag: "Success", snapshot: { source: value, values, extraEntries } };
  } catch {
    return { _tag: "Failure", reason: "invalidReflection" };
  }
};

export const inspectDenseArrayData = (value: unknown): DenseArrayInspection => {
  const inspection = inspectArrayData(value);
  if (inspection._tag === "Failure") {
    return inspection;
  }
  return inspection.snapshot.extraEntries.length === 0
    ? { _tag: "Success", values: inspection.snapshot.values }
    : {
        _tag: "Failure",
        reason: "invalidExtraProperty",
        key: inspection.snapshot.extraEntries[0]![0],
      };
};
