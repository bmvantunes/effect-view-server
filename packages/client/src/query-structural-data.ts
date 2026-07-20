export type PlainRecordSnapshot = {
  readonly source: object;
  readonly entries: ReadonlyArray<readonly [string, unknown]>;
};

export const hasPlainRecordPrototype = (value: unknown): value is object =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

export const plainRecordSnapshot = (
  value: unknown,
  invalidRecord: () => never,
  invalidProperty: () => never,
): PlainRecordSnapshot => {
  if (!hasPlainRecordPrototype(value) || Object.getOwnPropertySymbols(value).length > 0) {
    return invalidRecord();
  }
  const entries: Array<readonly [string, unknown]> = [];
  for (const key of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return invalidProperty();
    }
    entries.push([key, descriptor.value]);
  }
  return { source: value, entries };
};

export const denseArrayValues = (
  value: unknown,
  invalidArray: () => never,
  invalidEntry: () => never,
  invalidExtraProperty: () => never,
): ReadonlyArray<unknown> => {
  if (
    !Array.isArray(value) ||
    Object.getPrototypeOf(value) !== Array.prototype ||
    Object.getOwnPropertySymbols(value).length > 0
  ) {
    return invalidArray();
  }
  // An exact built-in Array prototype guarantees its non-configurable data descriptor.
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length")!;
  const length: number = lengthDescriptor.value;
  const values: Array<unknown> = [];
  const allowed = new Set(["length"]);
  for (let index = 0; index < length; index += 1) {
    const key = String(index);
    allowed.add(key);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return invalidEntry();
    }
    values.push(descriptor.value);
  }
  if (Object.getOwnPropertyNames(value).some((key) => !allowed.has(key))) {
    return invalidExtraProperty();
  }
  return values;
};
