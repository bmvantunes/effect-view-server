import type { TopicDefinitions, ViewServerRuntimeError } from "@effect-view-server/config";
import { Effect, Schema, SchemaAST } from "effect";
import { decodeAggregateValue, encodeAggregateValue } from "./protocol-aggregate-row-codec";
import { type ViewServerWireRow, ViewServerWireRowSchema } from "./protocol-event-schema";
import {
  decodeJsonFieldValue,
  decodeMaterializedJsonFieldValue,
  encodeJsonFieldValue,
  encodeTopicNamedJsonFieldValue,
  materializeJsonFieldValue,
} from "./protocol-json-field-codec";
import type {
  ViewServerEventGroupedQuery,
  ViewServerEventQuery,
  ViewServerWireAggregate,
} from "./protocol-query-schema";

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const rowJsonFieldContext = {
  invalid: invalidRow,
  invalidPrefix: "Invalid field",
  notJsonSafePrefix: "Field",
};

const defineEnumerableOwn = <Value>(
  target: Record<string, Value>,
  key: string,
  value: Value,
): void => {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const rowFieldIsOptional = (schema: Schema.Codec<unknown, unknown, never, never>): boolean =>
  SchemaAST.isOptional(schema.ast);

type RowKind = "row" | "grouped row";

type ViewServerUntrustedWireRow = Readonly<Record<string, unknown>>;

type InspectedRow = {
  readonly entries: ReadonlyArray<readonly [field: string, value: unknown]>;
  readonly fields: ReadonlySet<string>;
};

const rowKindTitle = (kind: RowKind): "Row" | "Grouped row" =>
  kind === "row" ? "Row" : "Grouped row";

const inspectRow = Effect.fn("ViewServerProtocol.row.inspect")(function* (
  topic: string,
  kind: RowKind,
  row: object,
): Effect.fn.Return<InspectedRow, ViewServerRuntimeError> {
  const keys = yield* Effect.try({
    try: () => Reflect.ownKeys(row),
    catch: () => invalidRow(topic, `Could not inspect ${kind} for topic ${topic}`),
  });
  const entries: Array<readonly [field: string, value: unknown]> = [];
  const fields = new Set<string>();
  for (const key of keys) {
    if (typeof key === "symbol") {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected ${kind} symbol field for topic ${topic}: ${String(key)}`),
      );
    }
    const descriptor = yield* Effect.try({
      try: () => Object.getOwnPropertyDescriptor(row, key),
      catch: () => invalidRow(topic, `Could not inspect ${kind} field for topic ${topic}: ${key}`),
    });
    if (descriptor === undefined) {
      return yield* Effect.fail(
        invalidRow(topic, `Could not inspect ${kind} field for topic ${topic}: ${key}`),
      );
    }
    if (descriptor.enumerable !== true) {
      return yield* Effect.fail(
        invalidRow(
          topic,
          `${rowKindTitle(kind)} field for topic ${topic} must be enumerable: ${key}`,
        ),
      );
    }
    if (!("value" in descriptor)) {
      return yield* Effect.fail(
        invalidRow(
          topic,
          `${rowKindTitle(kind)} field for topic ${topic} must be a data property: ${key}`,
        ),
      );
    }
    fields.add(key);
    entries.push([key, descriptor.value]);
  }
  return { entries, fields };
});

const isViewServerWireRow = Schema.is(ViewServerWireRowSchema);

const materializeWireRow = Effect.fn("ViewServerProtocol.row.materializeWire")(function* (
  topic: string,
  kind: RowKind,
  row: ViewServerUntrustedWireRow,
) {
  const materialized = yield* materializeJsonFieldValue(row, (message) =>
    invalidRow(topic, `Invalid ${kind} for topic ${topic}: ${message}`),
  );
  if (!isViewServerWireRow(materialized)) {
    return yield* Effect.fail(
      invalidRow(topic, `Invalid ${kind} for topic ${topic}: Expected a JSON object.`),
    );
  }
  return materialized;
});

export const isViewServerEventGroupedQuery = (
  query: ViewServerEventQuery,
): query is ViewServerEventGroupedQuery => Object.hasOwn(query, "groupBy");

export type ViewServerGroupedRowContract = {
  readonly aggregateAliases: ReadonlySet<string>;
  readonly aggregates: Readonly<Record<string, ViewServerWireAggregate | undefined>>;
  readonly groupFields: ReadonlySet<string>;
};

const compileViewServerAggregate = (
  aggregate: ViewServerWireAggregate | undefined,
): ViewServerWireAggregate | undefined =>
  aggregate === undefined
    ? undefined
    : aggregate.aggFunc === "count"
      ? Object.freeze({ aggFunc: aggregate.aggFunc })
      : Object.freeze({ aggFunc: aggregate.aggFunc, field: aggregate.field });

const compileViewServerAggregates = (
  aggregates: ViewServerEventGroupedQuery["aggregates"],
): Readonly<Record<string, ViewServerWireAggregate | undefined>> => {
  const compiled: Record<string, ViewServerWireAggregate | undefined> = {};
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    defineEnumerableOwn(compiled, alias, compileViewServerAggregate(aggregate));
  }
  return Object.freeze(compiled);
};

export const compileViewServerGroupedRowContract = (
  query: ViewServerEventGroupedQuery,
): ViewServerGroupedRowContract => {
  const aggregates = compileViewServerAggregates(query.aggregates);
  return {
    aggregateAliases: new Set(Object.keys(aggregates)),
    aggregates,
    groupFields: new Set(query.groupBy),
  };
};

export const encodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  selectedFields: ReadonlySet<string>,
  row: object,
) {
  const topicSchema = config.topics[topic]!.schema;
  const output: Record<string, Schema.Json> = {};
  const inspected = yield* inspectRow(topic, "row", row);
  for (const field of selectedFields) {
    const fieldSchema = topicSchema.fields[field]!;
    if (!inspected.fields.has(field) && !rowFieldIsOptional(fieldSchema)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const [field, value] of inspected.entries) {
    if (!selectedFields.has(field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected row field for topic ${topic}: ${field}`),
      );
    }
    const fieldSchema = topicSchema.fields[field]!;
    const encoded = yield* encodeTopicNamedJsonFieldValue(
      topic,
      field,
      fieldSchema,
      value,
      rowJsonFieldContext,
    );
    defineEnumerableOwn(output, field, encoded);
  }
  return output;
});

export const decodeMaterializedProjectedRow = Effect.fn(
  "ViewServerProtocol.row.project.decodeMaterialized",
)(function* <const Topics extends TopicDefinitions, Topic extends Extract<keyof Topics, string>>(
  config: { readonly topics: Topics },
  topic: Topic,
  selectedFields: ReadonlySet<string>,
  row: ViewServerWireRow,
) {
  const output: Record<string, unknown> = {};
  for (const field of selectedFields) {
    const fieldSchema = config.topics[topic]!.schema.fields[field]!;
    if (!Object.hasOwn(row, field) && !rowFieldIsOptional(fieldSchema)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (!selectedFields.has(field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected row field for topic ${topic}: ${field}`),
      );
    }
    const fieldSchema = config.topics[topic]!.schema.fields[field]!;
    const decoded = yield* decodeMaterializedJsonFieldValue(fieldSchema, value, {
      invalid: (message) => invalidRow(topic, `Invalid field ${field}: ${message}`),
    });
    defineEnumerableOwn(output, field, decoded);
  }
  return output;
});

export const decodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  selectedFields: ReadonlySet<string>,
  row: ViewServerUntrustedWireRow,
) {
  const materialized = yield* materializeWireRow(topic, "row", row);
  return yield* decodeMaterializedProjectedRow(config, topic, selectedFields, materialized);
});

export const encodeGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  contract: ViewServerGroupedRowContract,
  row: object,
) {
  const topicSchema = config.topics[topic]!.schema;
  const { aggregateAliases, aggregates, groupFields } = contract;
  const output: Record<string, Schema.Json> = {};
  const inspected = yield* inspectRow(topic, "grouped row", row);
  for (const field of groupFields) {
    const fieldSchema = topicSchema.fields[field]!;
    if (!inspected.fields.has(field) && !rowFieldIsOptional(fieldSchema)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const alias of aggregateAliases) {
    if (!inspected.fields.has(alias)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped aggregate for topic ${topic}: ${alias}`),
      );
    }
  }
  for (const [field, value] of inspected.entries) {
    if (groupFields.has(field)) {
      const fieldSchema = topicSchema.fields[field]!;
      const encoded = yield* encodeTopicNamedJsonFieldValue(
        topic,
        field,
        fieldSchema,
        value,
        rowJsonFieldContext,
      );
      defineEnumerableOwn(output, field, encoded);
    } else if (aggregateAliases.has(field)) {
      const aggregate = aggregates[field];
      if (aggregate === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Missing grouped aggregate definition for topic ${topic}: ${field}`),
        );
      }
      const encoded = yield* encodeAggregateValue(config, topic, field, aggregate, value);
      defineEnumerableOwn(output, field, encoded);
    } else {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  return output;
});

export const decodeMaterializedGroupedRow = Effect.fn(
  "ViewServerProtocol.row.grouped.decodeMaterialized",
)(function* <const Topics extends TopicDefinitions, Topic extends Extract<keyof Topics, string>>(
  config: { readonly topics: Topics },
  topic: Topic,
  contract: ViewServerGroupedRowContract,
  row: ViewServerWireRow,
) {
  const topicSchema = config.topics[topic]!.schema;
  const { aggregateAliases, aggregates, groupFields } = contract;
  const output: Record<string, unknown> = {};
  for (const field of groupFields) {
    const fieldSchema = topicSchema.fields[field]!;
    if (!Object.hasOwn(row, field) && !rowFieldIsOptional(fieldSchema)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const alias of aggregateAliases) {
    if (!Object.hasOwn(row, alias)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped aggregate for topic ${topic}: ${alias}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (groupFields.has(field)) {
      const fieldSchema = topicSchema.fields[field]!;
      const decoded = yield* decodeMaterializedJsonFieldValue(fieldSchema, value, {
        invalid: (message) => invalidRow(topic, `Invalid field ${field}: ${message}`),
      });
      defineEnumerableOwn(output, field, decoded);
    } else if (aggregateAliases.has(field)) {
      const aggregate = aggregates[field];
      if (aggregate === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Missing grouped aggregate definition for topic ${topic}: ${field}`),
        );
      }
      const decoded = yield* decodeAggregateValue(config, topic, field, aggregate, value);
      defineEnumerableOwn(output, field, decoded);
    } else {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  return output;
});

export const decodeGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  contract: ViewServerGroupedRowContract,
  row: ViewServerUntrustedWireRow,
) {
  const materialized = yield* materializeWireRow(topic, "grouped row", row);
  return yield* decodeMaterializedGroupedRow(config, topic, contract, materialized);
});

export const encodeSystemRow = Effect.fn("ViewServerProtocol.system.row.encode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: Row,
) {
  const encoded = yield* encodeJsonFieldValue(schema, row, {
    invalid: (message) => invalidRow(topic, `Invalid system row: ${message}`),
    notJsonSafe: (message) => invalidRow(topic, `System row is not JSON-safe: ${message}`),
  });
  return yield* Schema.decodeUnknownEffect(ViewServerWireRowSchema)(encoded).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid system row: ${error.message}`)),
  );
});

export const decodeSystemRow = Effect.fn("ViewServerProtocol.system.row.decode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: ViewServerUntrustedWireRow,
) {
  return yield* decodeJsonFieldValue(schema, row, {
    invalid: (message) => invalidRow(topic, `Invalid system row: ${message}`),
  });
});
