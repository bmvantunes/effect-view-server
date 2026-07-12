import { Result, Schema } from "effect";

const StrictJsonMaterializationReasonSchema = Schema.Literals([
  "unsupported-type",
  "non-finite-number",
  "unsupported-prototype",
  "cyclic-reference",
  "sparse-array",
  "extra-array-property",
  "symbol-key",
  "non-enumerable-property",
  "accessor-property",
  "reflection-failure",
]);

export type StrictJsonMaterializationReason = typeof StrictJsonMaterializationReasonSchema.Type;

export class StrictJsonMaterializationError extends Schema.TaggedErrorClass<StrictJsonMaterializationError>()(
  "StrictJsonMaterializationError",
  {
    path: Schema.String,
    reason: StrictJsonMaterializationReasonSchema,
    message: Schema.String,
  },
) {}

type StrictJsonResult = Result.Result<Schema.Json, StrictJsonMaterializationError>;

const simplePathKey = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

const appendPropertyPath = (path: string, key: string): string =>
  simplePathKey.test(key) ? `${path}.${key}` : `${path}[${JSON.stringify(key)}]`;

const appendSymbolPath = (path: string, key: symbol): string => `${path}[${String(key)}]`;

const unsupportedType = (path: string, valueType: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "unsupported-type",
      message: `Unsupported JSON value type "${valueType}" at ${path}.`,
    }),
  );

const nonFiniteNumber = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "non-finite-number",
      message: `Expected a finite JSON number at ${path}.`,
    }),
  );

const unsupportedPrototype = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "unsupported-prototype",
      message: `Expected a plain data record or dense array at ${path}.`,
    }),
  );

const cyclicReference = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "cyclic-reference",
      message: `Cyclic reference detected at ${path}.`,
    }),
  );

const sparseArray = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "sparse-array",
      message: `Sparse arrays are not valid JSON data at ${path}.`,
    }),
  );

const extraArrayProperty = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "extra-array-property",
      message: `Unexpected array property at ${path}.`,
    }),
  );

const symbolKey = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "symbol-key",
      message: `Symbol-keyed properties are not valid JSON data at ${path}.`,
    }),
  );

const nonEnumerableProperty = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "non-enumerable-property",
      message: `Expected an enumerable data property at ${path}.`,
    }),
  );

const accessorProperty = (path: string): StrictJsonResult =>
  Result.fail(
    StrictJsonMaterializationError.make({
      path,
      reason: "accessor-property",
      message: `Accessor properties are not valid JSON data at ${path}.`,
    }),
  );

const reflectionFailureError = (path: string): StrictJsonMaterializationError =>
  StrictJsonMaterializationError.make({
    path,
    reason: "reflection-failure",
    message: `Could not inspect JSON value at ${path}.`,
  });

const reflectionFailure = (path: string): StrictJsonResult =>
  Result.fail(reflectionFailureError(path));

const inspect = <A>(path: string, operation: () => A) =>
  Result.try({
    try: operation,
    catch: () => reflectionFailureError(path),
  });

const isArrayIndex = (key: string, length: number): boolean => {
  const index = Number(key);
  return Number.isInteger(index) && index >= 0 && index < length && String(index) === key;
};

const materializeDataProperty = (
  descriptor: PropertyDescriptor | undefined,
  path: string,
  active: WeakSet<object>,
): StrictJsonResult => {
  if (descriptor === undefined) {
    return reflectionFailure(path);
  }
  if (descriptor.enumerable !== true) {
    return nonEnumerableProperty(path);
  }
  if (!("value" in descriptor)) {
    return accessorProperty(path);
  }

  const value: unknown = descriptor.value;
  return materialize(value, path, active);
};

const materializeArray = (
  value: object,
  path: string,
  keys: ReadonlyArray<string | symbol>,
  active: WeakSet<object>,
): StrictJsonResult => {
  const lengthPath = appendPropertyPath(path, "length");
  const lengthResult = inspect(lengthPath, () => Reflect.get(value, "length"));
  if (Result.isFailure(lengthResult)) {
    return Result.fail(lengthResult.failure);
  }

  const lengthValue: unknown = lengthResult.success;
  if (typeof lengthValue !== "number") {
    return reflectionFailure(lengthPath);
  }

  const indices: Array<number> = [];
  for (const key of keys) {
    if (typeof key === "symbol") {
      return symbolKey(appendSymbolPath(path, key));
    }
    if (key === "length") {
      continue;
    }
    if (!isArrayIndex(key, lengthValue)) {
      return extraArrayProperty(appendPropertyPath(path, key));
    }
    indices.push(Number(key));
  }

  indices.sort((left, right) => left - right);
  let expectedIndex = 0;
  for (const index of indices) {
    if (index !== expectedIndex) {
      return sparseArray(`${path}[${expectedIndex}]`);
    }
    expectedIndex += 1;
  }
  if (expectedIndex !== lengthValue) {
    return sparseArray(`${path}[${expectedIndex}]`);
  }

  const output: Array<Schema.Json> = [];
  for (let index = 0; index < lengthValue; index += 1) {
    const itemPath = `${path}[${index}]`;
    const descriptorResult = inspect(itemPath, () =>
      Object.getOwnPropertyDescriptor(value, String(index)),
    );
    if (Result.isFailure(descriptorResult)) {
      return Result.fail(descriptorResult.failure);
    }

    const itemResult = materializeDataProperty(descriptorResult.success, itemPath, active);
    if (Result.isFailure(itemResult)) {
      return itemResult;
    }
    output.push(itemResult.success);
  }

  return Result.succeed(output);
};

const materializeRecord = (
  value: object,
  path: string,
  keys: ReadonlyArray<string | symbol>,
  active: WeakSet<object>,
): StrictJsonResult => {
  const output: Record<string, Schema.Json> = {};

  for (const key of keys) {
    if (typeof key === "symbol") {
      return symbolKey(appendSymbolPath(path, key));
    }

    const propertyPath = appendPropertyPath(path, key);
    const descriptorResult = inspect(propertyPath, () =>
      Object.getOwnPropertyDescriptor(value, key),
    );
    if (Result.isFailure(descriptorResult)) {
      return Result.fail(descriptorResult.failure);
    }

    const propertyResult = materializeDataProperty(descriptorResult.success, propertyPath, active);
    if (Result.isFailure(propertyResult)) {
      return propertyResult;
    }

    Object.defineProperty(output, key, {
      configurable: true,
      enumerable: true,
      value: propertyResult.success,
      writable: true,
    });
  }

  return Result.succeed(output);
};

const materializeObject = (
  value: object,
  path: string,
  active: WeakSet<object>,
): StrictJsonResult => {
  if (active.has(value)) {
    return cyclicReference(path);
  }
  active.add(value);

  const finish = (result: StrictJsonResult): StrictJsonResult => {
    active.delete(value);
    return result;
  };

  const isArrayResult = inspect(path, () => Array.isArray(value));
  if (Result.isFailure(isArrayResult)) {
    return finish(Result.fail(isArrayResult.failure));
  }

  const prototypeResult = inspect(path, () => Object.getPrototypeOf(value));
  if (Result.isFailure(prototypeResult)) {
    return finish(Result.fail(prototypeResult.failure));
  }

  const isArray = isArrayResult.success;
  const prototype = prototypeResult.success;
  if (
    (isArray && prototype !== Array.prototype) ||
    (!isArray && prototype !== Object.prototype && prototype !== null)
  ) {
    return finish(unsupportedPrototype(path));
  }

  const keysResult = inspect(path, () => Reflect.ownKeys(value));
  if (Result.isFailure(keysResult)) {
    return finish(Result.fail(keysResult.failure));
  }

  return finish(
    isArray
      ? materializeArray(value, path, keysResult.success, active)
      : materializeRecord(value, path, keysResult.success, active),
  );
};

const materialize = (value: unknown, path: string, active: WeakSet<object>): StrictJsonResult => {
  if (value === null) {
    return Result.succeed(null);
  }

  if (typeof value === "string" || typeof value === "boolean") {
    return Result.succeed(value);
  }
  if (typeof value === "number") {
    return Number.isFinite(value)
      ? Result.succeed(Object.is(value, -0) ? 0 : value)
      : nonFiniteNumber(path);
  }
  if (typeof value === "object") {
    return materializeObject(value, path, active);
  }
  return unsupportedType(path, typeof value);
};

export const materializeStrictJson = (
  value: unknown,
): Result.Result<Schema.Json, StrictJsonMaterializationError> =>
  materialize(value, "$", new WeakSet());
