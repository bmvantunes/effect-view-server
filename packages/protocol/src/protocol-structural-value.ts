export const isProtocolPlainRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

export type ProtocolOwnDataValue = {
  readonly found: boolean;
  readonly value: unknown;
};

export const protocolOwnDataValue = (
  value: Readonly<Record<string, unknown>>,
  key: string,
): ProtocolOwnDataValue => {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor !== undefined && descriptor.enumerable && "value" in descriptor
    ? { found: true, value: descriptor.value }
    : { found: false, value: undefined };
};

export const protocolRecordDataEntries = (
  value: Readonly<Record<string, unknown>>,
): ReadonlyArray<readonly [string, unknown]> | undefined => {
  if (Object.getOwnPropertySymbols(value).length > 0) {
    return undefined;
  }
  const entries: Array<readonly [string, unknown]> = [];
  for (const key of Object.getOwnPropertyNames(value)) {
    const field = protocolOwnDataValue(value, key);
    if (!field.found) {
      return undefined;
    }
    entries.push([key, field.value]);
  }
  return entries;
};

export const protocolHasOnlyDataKeys = (
  value: Readonly<Record<string, unknown>>,
  allowed: ReadonlySet<string>,
): boolean => {
  const entries = protocolRecordDataEntries(value);
  return entries !== undefined && entries.every(([key]) => allowed.has(key));
};

export const protocolHasExactDataKeys = (
  value: Readonly<Record<string, unknown>>,
  expected: ReadonlySet<string>,
): boolean => {
  const entries = protocolRecordDataEntries(value);
  return (
    entries !== undefined &&
    entries.length === expected.size &&
    entries.every(([key]) => expected.has(key))
  );
};

export const protocolDenseArray = (value: unknown): ReadonlyArray<unknown> | undefined => {
  if (!Array.isArray(value) || Object.getPrototypeOf(value) !== Array.prototype) {
    return undefined;
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    return undefined;
  }
  const output: Array<unknown> = [];
  const allowed = new Set(["length"]);
  for (let index = 0; index < value.length; index += 1) {
    const key = String(index);
    allowed.add(key);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return undefined;
    }
    output.push(descriptor.value);
  }
  return Object.getOwnPropertyNames(value).every((key) => allowed.has(key)) ? output : undefined;
};
