import type { TopicDefinitions, ViewServerRuntimeError } from "@effect-view-server/config";
import { viewServerSchemaFieldMetadata } from "@effect-view-server/config";
import { Effect, Schema, SchemaAST } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  decodeMaterializedJsonFieldValue,
  encodeContextualJsonFieldValue,
  type JsonFieldSchema,
} from "./protocol-json-field-codec";
import type { ViewServerWireAggregate } from "./protocol-query-schema";

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isBigIntFieldSchema = (schema: JsonFieldSchema): boolean =>
  viewServerSchemaFieldMetadata(schema).sumResultKind === "bigint";

const bigintPattern = /^-?\d+$/;

type BigIntAggregateEnvelope = {
  readonly _viewServerAggregate: "bigint";
  readonly value: string;
};

type BigDecimalAggregateEnvelope = {
  readonly _viewServerAggregate: "bigdecimal";
  readonly value: string;
};

type JsonAggregateEnvelope<Value> = {
  readonly _viewServerAggregate: "json";
  readonly value: Value;
};

type UndefinedAggregateEnvelope = {
  readonly _viewServerAggregate: "undefined";
};

type AggregateEnvelope =
  | BigIntAggregateEnvelope
  | BigDecimalAggregateEnvelope
  | JsonAggregateEnvelope<Schema.Json>
  | UndefinedAggregateEnvelope;

const isUndefinedAggregateEnvelope = (value: Schema.Json): value is UndefinedAggregateEnvelope =>
  isRecord(value) &&
  value["_viewServerAggregate"] === "undefined" &&
  !Object.hasOwn(value, "value");

const isAggregateEnvelope = (value: Schema.Json): value is AggregateEnvelope =>
  isUndefinedAggregateEnvelope(value) ||
  (isRecord(value) &&
    ((value["_viewServerAggregate"] === "bigint" && typeof value["value"] === "string") ||
      (value["_viewServerAggregate"] === "bigdecimal" && typeof value["value"] === "string") ||
      (value["_viewServerAggregate"] === "json" && Object.hasOwn(value, "value"))));

const encodeJsonAggregateEnvelope = (value: Schema.Json): AggregateEnvelope => ({
  _viewServerAggregate: "json",
  value,
});

const encodeBigIntAggregateEnvelope = (value: bigint): AggregateEnvelope => ({
  _viewServerAggregate: "bigint",
  value: value.toString(),
});

const encodeBigDecimalAggregateEnvelope = (value: BigDecimal.BigDecimal): AggregateEnvelope => ({
  _viewServerAggregate: "bigdecimal",
  value: BigDecimal.format(value),
});

const encodeUndefinedAggregateEnvelope = (): AggregateEnvelope => ({
  _viewServerAggregate: "undefined",
});

const decodeAggregateEnvelope = Effect.fn("ViewServerProtocol.row.aggregate.envelope.decode")(
  function* (topic: string, field: string, value: Schema.Json) {
    if (!isAggregateEnvelope(value)) {
      return yield* Effect.fail(
        invalidRow(topic, `Aggregate ${field} must be a View Server aggregate envelope.`),
      );
    }
    return value;
  },
);

const encodeAggregateJsonFieldValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.jsonField.encode",
)(function* (topic: string, field: string, schema: JsonFieldSchema, value: unknown) {
  return yield* encodeContextualJsonFieldValue(schema, value, {
    invalid: (message) => invalidRow(topic, message),
    invalidMessage: (message) => `Invalid field ${field}: ${message}`,
    notJsonSafe: (message) => invalidRow(topic, message),
    notJsonSafeMessage: (message) => `Field ${field} is not JSON-safe: ${message}`,
  });
});

const encodeBigIntAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.bigint.encode")(
  function* (topic: string, field: string, value: unknown) {
    if (typeof value !== "bigint") {
      return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a bigint.`));
    }
    return encodeBigIntAggregateEnvelope(value);
  },
);

const decodeBigIntAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.bigint.decode")(
  function* (topic: string, field: string, value: Schema.Json) {
    const envelope = yield* decodeAggregateEnvelope(topic, field, value);
    if (envelope._viewServerAggregate !== "bigint" || !bigintPattern.test(envelope.value)) {
      return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a bigint envelope.`));
    }
    return BigInt(envelope.value);
  },
);

const encodeBigDecimalAggregateValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.bigDecimal.encode",
)(function* (topic: string, field: string, value: unknown) {
  if (!BigDecimal.isBigDecimal(value)) {
    return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a BigDecimal.`));
  }
  return encodeBigDecimalAggregateEnvelope(value);
});

const decodeBigDecimalAggregateValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.bigDecimal.decode",
)(function* (topic: string, field: string, value: Schema.Json) {
  const envelope = yield* decodeAggregateEnvelope(topic, field, value);
  if (envelope._viewServerAggregate !== "bigdecimal") {
    return yield* Effect.fail(
      invalidRow(topic, `Aggregate ${field} must be a BigDecimal envelope.`),
    );
  }
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(Schema.BigDecimal))(
    envelope.value,
  ).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid aggregate ${field}: ${error.message}`)),
  );
});

const encodeJsonAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.json.encode")(
  function* (topic: string, field: string, schema: JsonFieldSchema, value: unknown) {
    const encoded = yield* encodeAggregateJsonFieldValue(topic, field, schema, value);
    return encodeJsonAggregateEnvelope(encoded);
  },
);

const decodeJsonAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.json.decode")(
  function* (topic: string, field: string, schema: JsonFieldSchema, value: Schema.Json) {
    const envelope = yield* decodeAggregateEnvelope(topic, field, value);
    if (envelope._viewServerAggregate !== "json") {
      return yield* Effect.fail(
        invalidRow(topic, `Aggregate ${field} must be a JSON aggregate envelope.`),
      );
    }
    return yield* decodeMaterializedJsonFieldValue(schema, envelope.value, {
      invalid: (message) => invalidRow(topic, `Invalid field ${field}: ${message}`),
    });
  },
);

const aggregateFieldSchema = Effect.fn("ViewServerProtocol.row.aggregate.fieldSchema")(function* <
  const Topics extends TopicDefinitions,
>(config: { readonly topics: Topics }, topic: Extract<keyof Topics, string>, field: string) {
  const fieldSchema = config.topics[topic]!.schema.fields[field];
  if (fieldSchema === undefined) {
    return yield* Effect.fail(
      invalidRow(topic, `Aggregate references unknown field for topic ${topic}: ${field}`),
    );
  }
  return fieldSchema;
});

const undefinedAggregateFieldAcceptance = new WeakMap<JsonFieldSchema, boolean>();

const fieldSchemaAllowsUndefinedAggregate = (schema: JsonFieldSchema): boolean => {
  const cached = undefinedAggregateFieldAcceptance.get(schema);
  if (cached !== undefined) {
    return cached;
  }
  const accepted = SchemaAST.isOptional(schema.ast) || Schema.is(schema)(undefined);
  undefinedAggregateFieldAcceptance.set(schema, accepted);
  return accepted;
};

const invalidUndefinedAggregate = (
  topic: string,
  field: string,
  aggregateFunction: "min" | "max",
  aggregateField: string,
): ViewServerRuntimeError =>
  invalidRow(
    topic,
    `Invalid field ${field}: aggregate ${aggregateFunction} cannot be undefined because ${aggregateField} is required.`,
  );

export const encodeAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
  aggregate: ViewServerWireAggregate,
  value: unknown,
) {
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return yield* encodeBigIntAggregateValue(topic, field, value);
  }
  const fieldSchema = yield* aggregateFieldSchema(config, topic, aggregate.field);
  if ((aggregate.aggFunc === "min" || aggregate.aggFunc === "max") && value === undefined) {
    if (!fieldSchemaAllowsUndefinedAggregate(fieldSchema)) {
      return yield* Effect.fail(
        invalidUndefinedAggregate(topic, field, aggregate.aggFunc, aggregate.field),
      );
    }
    return encodeUndefinedAggregateEnvelope();
  }
  if (aggregate.aggFunc === "avg") {
    return yield* encodeBigDecimalAggregateValue(topic, field, value);
  }
  if (aggregate.aggFunc === "sum") {
    if (isBigIntFieldSchema(fieldSchema)) {
      return yield* encodeBigIntAggregateValue(topic, field, value);
    }
    return yield* encodeBigDecimalAggregateValue(topic, field, value);
  }
  return yield* encodeJsonAggregateValue(topic, field, fieldSchema, value);
});

export const decodeAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.decode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
  aggregate: ViewServerWireAggregate,
  value: Schema.Json,
) {
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return yield* decodeBigIntAggregateValue(topic, field, value);
  }
  const fieldSchema = yield* aggregateFieldSchema(config, topic, aggregate.field);
  if (
    (aggregate.aggFunc === "min" || aggregate.aggFunc === "max") &&
    isUndefinedAggregateEnvelope(value)
  ) {
    if (!fieldSchemaAllowsUndefinedAggregate(fieldSchema)) {
      return yield* Effect.fail(
        invalidUndefinedAggregate(topic, field, aggregate.aggFunc, aggregate.field),
      );
    }
    return undefined;
  }
  if (aggregate.aggFunc === "avg") {
    return yield* decodeBigDecimalAggregateValue(topic, field, value);
  }
  if (aggregate.aggFunc === "sum") {
    if (isBigIntFieldSchema(fieldSchema)) {
      return yield* decodeBigIntAggregateValue(topic, field, value);
    }
    return yield* decodeBigDecimalAggregateValue(topic, field, value);
  }
  return yield* decodeJsonAggregateValue(topic, field, fieldSchema, value);
});
