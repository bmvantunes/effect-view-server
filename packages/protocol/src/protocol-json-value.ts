import type { ViewServerRuntimeError } from "@effect-view-server/config";
import { Effect, type Schema } from "effect";
import {
  isProtocolPlainRecord,
  protocolDenseArray,
  protocolRecordDataEntries,
} from "./protocol-structural-value";

type JsonFrame =
  | { readonly _tag: "enter"; readonly value: unknown }
  | { readonly _tag: "exit"; readonly value: object };

const recordValues = (
  value: Readonly<Record<string, unknown>>,
): ReadonlyArray<unknown> | undefined => {
  const entries = protocolRecordDataEntries(value);
  return entries?.map(([, entry]) => entry);
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
      ? protocolDenseArray(current)
      : isProtocolPlainRecord(current)
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
