import type { ViewServerRuntimeError } from "@effect-view-server/config";
import { Effect, type Schema } from "effect";

type JsonFrame =
  | { readonly _tag: "enter"; readonly value: unknown }
  | { readonly _tag: "exit"; readonly value: object };

const isPlainRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  Object.getPrototypeOf(value) === Object.prototype;

const denseArray = (value: unknown): ReadonlyArray<unknown> | undefined => {
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

const recordValues = (
  value: Readonly<Record<string, unknown>>,
): ReadonlyArray<unknown> | undefined => {
  if (Object.getOwnPropertySymbols(value).length > 0) {
    return undefined;
  }
  const values: Array<unknown> = [];
  for (const key of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !descriptor.enumerable || !("value" in descriptor)) {
      return undefined;
    }
    values.push(descriptor.value);
  }
  return values;
};

export const isProtocolJson = (value: unknown): value is Schema.Json => {
  const frames: Array<JsonFrame> = [{ _tag: "enter", value }];
  const active = new WeakSet<object>();
  const complete = new WeakSet<object>();
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exit") {
      active.delete(frame.value);
      complete.add(frame.value);
      continue;
    }
    const current = frame.value;
    if (
      current === null ||
      typeof current === "string" ||
      typeof current === "boolean" ||
      (typeof current === "number" && Number.isFinite(current))
    ) {
      continue;
    }
    if (typeof current !== "object" || active.has(current)) {
      return false;
    }
    if (complete.has(current)) {
      continue;
    }
    const children = Array.isArray(current)
      ? denseArray(current)
      : isPlainRecord(current)
        ? recordValues(current)
        : undefined;
    if (children === undefined) {
      return false;
    }
    active.add(current);
    frames.push({ _tag: "exit", value: current });
    for (let index = children.length - 1; index >= 0; index -= 1) {
      frames.push({ _tag: "enter", value: children[index] });
    }
  }
  return true;
};

export const requireProtocolJson = Effect.fn("ViewServerProtocol.json.require")(function* <
  const Value,
>(topic: string, value: Value): Effect.fn.Return<Value & Schema.Json, ViewServerRuntimeError> {
  if (!isProtocolJson(value)) {
    const error: ViewServerRuntimeError = {
      _tag: "ViewServerRuntimeError",
      code: "InvalidQuery",
      message: "Encoded filter is not JSON-safe",
      topic,
    };
    return yield* Effect.fail(error);
  }
  return value;
});

export const requireProtocolJsonArray = Effect.fn("ViewServerProtocol.jsonArray.require")(
  function* (
    topic: string,
    value: ReadonlyArray<unknown>,
  ): Effect.fn.Return<ReadonlyArray<Schema.Json>, ViewServerRuntimeError> {
    const isJsonArray = (
      candidate: ReadonlyArray<unknown>,
    ): candidate is ReadonlyArray<Schema.Json> => isProtocolJson(candidate);
    if (!isJsonArray(value)) {
      const error: ViewServerRuntimeError = {
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "Encoded filter is not JSON-safe",
        topic,
      };
      return yield* Effect.fail(error);
    }
    return value;
  },
);
