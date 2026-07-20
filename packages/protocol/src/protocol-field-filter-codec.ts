import type { RowSchema, ViewServerRuntimeError } from "@effect-view-server/config";
import { inspectWireSafeBigDecimal } from "@effect-view-server/effect-utils";
import { Effect } from "effect";
import { make as makeBigDecimal } from "effect/BigDecimal";
import {
  decodeTopicNamedJsonFieldValue,
  encodeTopicNamedJsonFieldValue,
  type JsonFieldSchema,
} from "./protocol-json-field-codec";
import {
  protocolFilterFieldSchema,
  protocolNumericOperandSchema,
} from "./protocol-filter-field-schema";
import { requireProtocolJsonArray } from "./protocol-json-value";
import {
  protocolDenseArray,
  protocolRecordSnapshot,
  protocolSnapshotDataValue,
  protocolSnapshotHasExactDataKeys,
  type ProtocolRecordSnapshot,
} from "./protocol-structural-value";

type Direction = "encode" | "decode";

const invalidQuery = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

const filterJsonFieldContext = {
  invalid: invalidQuery,
  invalidPrefix: "Invalid filter for",
  notJsonSafePrefix: "Filter",
};

const ownValue = (snapshot: ProtocolRecordSnapshot, key: string): unknown =>
  protocolSnapshotDataValue(snapshot, key);

const hasKey = (snapshot: ProtocolRecordSnapshot, key: string): boolean =>
  snapshot.entries.some(([entryKey]) => entryKey === key);

const exactKeys = (snapshot: ProtocolRecordSnapshot, allowed: ReadonlySet<string>): boolean =>
  protocolSnapshotHasExactDataKeys(snapshot, allowed);

const uniqueValues = (values: ReadonlyArray<unknown>): ReadonlyArray<unknown> => {
  const unique: Array<unknown> = [];
  const seen = new Set<unknown>();
  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }
    seen.add(value);
    unique.push(value);
  }
  return unique;
};

const ownFilterBigDecimal = Effect.fn("ViewServerProtocol.filter.bigDecimal.own")(function* (
  topic: string,
  field: string,
  value: unknown,
) {
  const inspection = inspectWireSafeBigDecimal(value);
  if (inspection._tag === "ReflectionFailure") {
    return yield* Effect.fail(
      invalidQuery(topic, `Filter condition ${field} operand could not be inspected`),
    );
  }
  if (inspection._tag === "UnsafeBigDecimal") {
    return yield* Effect.fail(
      invalidQuery(topic, `Filter condition ${field} BigDecimal operand is not wire-safe`),
    );
  }
  return inspection._tag === "Success"
    ? makeBigDecimal(inspection.coefficient, inspection.scale)
    : value;
});

const transformFieldValue = Effect.fn("ViewServerProtocol.filter.fieldValue.transform")(function* (
  direction: Direction,
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  const ownedValue = yield* ownFilterBigDecimal(topic, field, value);
  const transformed =
    direction === "encode"
      ? yield* encodeTopicNamedJsonFieldValue(
          topic,
          field,
          schema,
          ownedValue,
          filterJsonFieldContext,
        )
      : yield* decodeTopicNamedJsonFieldValue(
          topic,
          field,
          schema,
          ownedValue,
          filterJsonFieldContext,
        );
  return yield* ownFilterBigDecimal(topic, field, transformed);
});

type TransformFrame =
  | { readonly _tag: "enter"; readonly value: unknown }
  | {
      readonly _tag: "exit";
      readonly source: Readonly<Record<string, unknown>>;
      readonly type: "AND" | "OR" | "NOT";
      readonly childCount: number;
    };

type TransformState = {
  readonly active: WeakSet<object>;
  readonly memo: WeakMap<object, unknown>;
};

const transformCondition = Effect.fn("ViewServerProtocol.filter.condition.transform")(function* (
  direction: Direction,
  topic: string,
  rowSchema: RowSchema,
  condition: ProtocolRecordSnapshot,
) {
  const field = ownValue(condition, "field");
  const type = ownValue(condition, "type");
  if (typeof field !== "string" || typeof type !== "string") {
    return yield* Effect.fail(invalidQuery(topic, "Filter conditions require field and type"));
  }
  const resolved = protocolFilterFieldSchema(rowSchema, field);
  if (resolved === undefined) {
    return yield* Effect.fail(
      invalidQuery(topic, `Query references an unknown or non-filterable field: ${field}`),
    );
  }
  const blank = type === "blank" || type === "notBlank";
  const inRange = type === "inRange";
  const text =
    type === "contains" || type === "notContains" || type === "startsWith" || type === "endsWith";
  const equality = type === "equals" || type === "notEqual" || type === "in";
  const supportsTextMatching = text || (equality && resolved.supportsText);
  const numeric =
    type === "greaterThan" ||
    type === "greaterThanOrEqual" ||
    type === "lessThan" ||
    type === "lessThanOrEqual" ||
    inRange;
  if (!blank && !text && !equality && !numeric) {
    return yield* Effect.fail(invalidQuery(topic, `Unsupported filter condition type: ${type}`));
  }
  const expected = new Set(["field", "type"]);
  if (!blank) {
    expected.add("filter");
  }
  if (inRange) {
    expected.add("filterTo");
  }
  if (supportsTextMatching) {
    if (hasKey(condition, "caseSensitive")) {
      expected.add("caseSensitive");
    }
    if (hasKey(condition, "accentSensitive")) {
      expected.add("accentSensitive");
    }
  }
  if (!exactKeys(condition, expected)) {
    return yield* Effect.fail(invalidQuery(topic, `Filter condition ${field} has invalid keys`));
  }
  const output: Record<string, unknown> = { field, type };
  if (hasKey(condition, "caseSensitive")) {
    const caseSensitive = ownValue(condition, "caseSensitive");
    if (typeof caseSensitive !== "boolean") {
      return yield* Effect.fail(
        invalidQuery(topic, `Filter condition ${field} caseSensitive must be a boolean`),
      );
    }
    output["caseSensitive"] = caseSensitive;
  }
  if (hasKey(condition, "accentSensitive")) {
    const accentSensitive = ownValue(condition, "accentSensitive");
    if (typeof accentSensitive !== "boolean") {
      return yield* Effect.fail(
        invalidQuery(topic, `Filter condition ${field} accentSensitive must be a boolean`),
      );
    }
    output["accentSensitive"] = accentSensitive;
  }
  if (blank) {
    return Object.freeze(output);
  }
  const filter = ownValue(condition, "filter");
  if (type === "in") {
    const candidates = protocolDenseArray(filter);
    if (candidates === undefined) {
      return yield* Effect.fail(
        invalidQuery(topic, `Filter condition ${field} in must be an array`),
      );
    }
    output["filter"] = Object.freeze(
      yield* Effect.forEach(candidates, (candidate) =>
        transformFieldValue(direction, topic, field, resolved.schema, candidate),
      ),
    );
    return Object.freeze(output);
  }
  if (text) {
    if (!resolved.supportsText) {
      return yield* Effect.fail(invalidQuery(topic, `Filter ${field} does not support ${type}`));
    }
    if (typeof filter !== "string") {
      return yield* Effect.fail(
        invalidQuery(topic, `Filter condition ${field} ${type} requires a string`),
      );
    }
    output["filter"] = filter;
    return Object.freeze(output);
  }
  if (numeric && resolved.numericKinds.size === 0) {
    return yield* Effect.fail(
      invalidQuery(topic, `Filter ${field} does not support range operators`),
    );
  }
  const operandSchema = numeric ? protocolNumericOperandSchema(resolved) : resolved.schema;
  const transformedFilter = yield* transformFieldValue(
    direction,
    topic,
    field,
    operandSchema,
    filter,
  );
  output["filter"] = transformedFilter;
  if (inRange) {
    const filterTo = ownValue(condition, "filterTo");
    const transformedFilterTo = yield* transformFieldValue(
      direction,
      topic,
      field,
      operandSchema,
      filterTo,
    );
    output["filterTo"] = transformedFilterTo;
  }
  return Object.freeze(output);
});

const transformExpression = Effect.fn("ViewServerProtocol.filter.expression.transform")(function* (
  direction: Direction,
  topic: string,
  rowSchema: RowSchema,
  input: unknown,
  state: TransformState,
) {
  const frames: Array<TransformFrame> = [{ _tag: "enter", value: input }];
  const results: Array<unknown> = [];
  while (frames.length > 0) {
    const frame = frames.pop()!;
    if (frame._tag === "exit") {
      const children = results.splice(results.length - frame.childCount, frame.childCount);
      const uniqueChildren = frame.type === "NOT" ? children : uniqueValues(children);
      const output =
        frame.type === "NOT"
          ? Object.freeze({ type: "NOT", condition: children[0] })
          : Object.freeze({ type: frame.type, conditions: Object.freeze(uniqueChildren) });
      state.active.delete(frame.source);
      state.memo.set(frame.source, output);
      results.push(output);
      continue;
    }
    const expression = frame.value;
    if (typeof expression !== "object" || expression === null) {
      return yield* Effect.fail(invalidQuery(topic, "Every filter expression must be an object"));
    }
    const cached = state.memo.get(expression);
    if (cached !== undefined) {
      results.push(cached);
      continue;
    }
    if (state.active.has(expression)) {
      return yield* Effect.fail(invalidQuery(topic, "Filter expressions must not contain cycles"));
    }
    const snapshot = protocolRecordSnapshot(expression);
    if (snapshot === undefined) {
      return yield* Effect.fail(invalidQuery(topic, "Every filter expression must be an object"));
    }
    const type = ownValue(snapshot, "type");
    if (type === "AND" || type === "OR") {
      if (!exactKeys(snapshot, new Set(["type", "conditions"]))) {
        return yield* Effect.fail(invalidQuery(topic, `Filter group ${type} has invalid keys`));
      }
      const children = protocolDenseArray(ownValue(snapshot, "conditions"));
      if (children === undefined) {
        return yield* Effect.fail(
          invalidQuery(topic, `Filter group ${type} conditions must be an array`),
        );
      }
      state.active.add(expression);
      frames.push({
        _tag: "exit",
        source: snapshot.source,
        type,
        childCount: children.length,
      });
      for (let index = children.length - 1; index >= 0; index -= 1) {
        frames.push({ _tag: "enter", value: children[index] });
      }
      continue;
    }
    if (type === "NOT") {
      if (!exactKeys(snapshot, new Set(["type", "condition"]))) {
        return yield* Effect.fail(invalidQuery(topic, "Filter NOT has invalid keys"));
      }
      state.active.add(expression);
      frames.push({ _tag: "exit", source: snapshot.source, type: "NOT", childCount: 1 });
      frames.push({ _tag: "enter", value: ownValue(snapshot, "condition") });
      continue;
    }
    const transformed = yield* transformCondition(direction, topic, rowSchema, snapshot);
    state.memo.set(expression, transformed);
    results.push(transformed);
  }
  return results[0];
});

const transformWhere = Effect.fn("ViewServerProtocol.filter.where.transform")(function* (
  direction: Direction,
  topic: string,
  rowSchema: RowSchema,
  where: ReadonlyArray<unknown> | undefined,
) {
  if (where === undefined) {
    return undefined;
  }
  const roots = protocolDenseArray(where);
  if (roots === undefined) {
    return yield* Effect.fail(invalidQuery(topic, "Query where must be an array"));
  }
  const state: TransformState = {
    active: new WeakSet(),
    memo: new WeakMap(),
  };
  const transformed: Array<unknown> = [];
  for (const expression of roots) {
    transformed.push(yield* transformExpression(direction, topic, rowSchema, expression, state));
  }
  return Object.freeze(transformed);
});

export const encodeWhere = Effect.fn("ViewServerProtocol.filter.where.encode")(function* (
  topic: string,
  rowSchema: RowSchema,
  where: ReadonlyArray<unknown> | undefined,
) {
  const encoded = yield* transformWhere("encode", topic, rowSchema, where);
  if (encoded === undefined) {
    return undefined;
  }
  return yield* requireProtocolJsonArray(topic, encoded);
});

export const decodeWhere = Effect.fn("ViewServerProtocol.filter.where.decode")(function* (
  topic: string,
  rowSchema: RowSchema,
  where: ReadonlyArray<unknown> | undefined,
) {
  return yield* transformWhere("decode", topic, rowSchema, where);
});
