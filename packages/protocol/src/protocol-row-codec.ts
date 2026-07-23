import {
  viewServerSchemaFieldMetadata,
  type ExactLiveQueryInputForTopic,
  type LiveQuery,
  type LiveQueryRow,
  type TopicDefinitions,
  type TopicRow,
  type ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Effect, Function, Result, Schema, SchemaAST } from "effect";
import * as BigDecimal from "effect/BigDecimal";
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
  ViewServerEventRawQuery,
  ViewServerWireAggregate,
} from "./protocol-query-schema";

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const invalidRowContract = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message: `Could not compile the live-event row contract for topic ${topic}`,
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

const ownRowFieldSchema = (
  fields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
  field: string,
): Schema.Codec<unknown, unknown, never, never> | undefined =>
  Object.hasOwn(fields, field) ? fields[field] : undefined;

type RowKind = "row" | "grouped row";

type ViewServerUntrustedWireRow = Readonly<Record<string, unknown>>;

type DecodedRowProof<Row extends object> = (row: object) => row is Row;

type DecodedFieldProof = {
  readonly field: string;
  readonly isValue: (value: unknown) => boolean;
  readonly required: boolean;
};

export type ViewServerEventRowPlan<Row extends object> = {
  readonly decodeMaterialized: (
    row: ViewServerWireRow,
  ) => Effect.Effect<Row, ViewServerRuntimeError>;
  readonly encode: (row: Row) => Effect.Effect<ViewServerWireRow, ViewServerRuntimeError>;
};

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

type ViewServerProjectedRowContract = {
  readonly fieldSchemas: ReadonlyMap<
    string,
    Schema.Codec<unknown, unknown, never, never> | undefined
  >;
  readonly selectedFields: ReadonlySet<string>;
};

type ViewServerCompiledGroupedRowContract = ViewServerGroupedRowContract & {
  readonly groupFieldSchemas: ReadonlyMap<
    string,
    Schema.Codec<unknown, unknown, never, never> | undefined
  >;
};

const compileProjectedRowContract = (
  fields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
  selectedFields: ReadonlySet<string>,
): ViewServerProjectedRowContract => ({
  fieldSchemas: new Map(
    [...selectedFields].map((field) => [field, ownRowFieldSchema(fields, field)]),
  ),
  selectedFields,
});

const compileGroupedRowContractForTopic = (
  fields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
  contract: ViewServerGroupedRowContract,
): ViewServerCompiledGroupedRowContract => ({
  ...contract,
  groupFieldSchemas: new Map(
    [...contract.groupFields].map((field) => [field, ownRowFieldSchema(fields, field)]),
  ),
});

const compileDecodedRowProof = <Row extends object>(
  fields: ReadonlyArray<DecodedFieldProof>,
): DecodedRowProof<Row> => {
  const fieldNames = new Set(fields.map((field) => field.field));
  return (row): row is Row =>
    fields.every((field) =>
      Object.hasOwn(row, field.field)
        ? field.isValue(Reflect.get(row, field.field))
        : !field.required,
    ) && Object.keys(row).every((field) => fieldNames.has(field));
};

const decodedTopicFieldProof = (
  field: string,
  schema: Schema.Codec<unknown, unknown, never, never> | undefined,
): DecodedFieldProof | undefined =>
  schema === undefined
    ? undefined
    : {
        field,
        isValue: Schema.is(schema),
        required: !rowFieldIsOptional(schema),
      };

const decodedAggregateValueProof = (
  fields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
  aggregate: ViewServerWireAggregate | undefined,
): ((value: unknown) => boolean) | undefined => {
  if (aggregate === undefined) {
    return undefined;
  }
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return (value) => typeof value === "bigint";
  }
  if (aggregate.aggFunc === "avg") {
    return BigDecimal.isBigDecimal;
  }
  const fieldSchema = ownRowFieldSchema(fields, aggregate.field);
  if (fieldSchema === undefined) {
    return undefined;
  }
  if (aggregate.aggFunc === "sum") {
    return viewServerSchemaFieldMetadata(fieldSchema).sumResultKind === "bigint"
      ? (value) => typeof value === "bigint"
      : BigDecimal.isBigDecimal;
  }
  const isFieldValue = Schema.is(fieldSchema);
  return (aggregate.aggFunc === "min" || aggregate.aggFunc === "max") &&
    SchemaAST.isOptional(fieldSchema.ast)
    ? (value) => value === undefined || isFieldValue(value)
    : isFieldValue;
};

const compileProjectedRowProof = <Row extends object>(
  contract: ViewServerProjectedRowContract,
): DecodedRowProof<Row> => {
  const fields: Array<DecodedFieldProof> = [];
  for (const [field, schema] of contract.fieldSchemas) {
    const proof = decodedTopicFieldProof(field, schema);
    if (proof !== undefined) {
      fields.push(proof);
    }
  }
  return compileDecodedRowProof<Row>(fields);
};

const compileGroupedRowProof = <Row extends object>(
  topicFields: Readonly<Record<string, Schema.Codec<unknown, unknown, never, never>>>,
  contract: ViewServerCompiledGroupedRowContract,
): DecodedRowProof<Row> => {
  const fields: Array<DecodedFieldProof> = [];
  for (const [field, schema] of contract.groupFieldSchemas) {
    const proof = decodedTopicFieldProof(field, schema);
    if (proof !== undefined) {
      fields.push(proof);
    }
  }
  for (const alias of contract.aggregateAliases) {
    const isValue = decodedAggregateValueProof(topicFields, contract.aggregates[alias]);
    if (isValue !== undefined) {
      fields.push({ field: alias, isValue, required: true });
    }
  }
  return compileDecodedRowProof<Row>(fields);
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

const encodeProjectedRowWithContract = Effect.fn(
  "ViewServerProtocol.row.project.encodeWithContract",
)(function* (topic: string, contract: ViewServerProjectedRowContract, row: object) {
  const { fieldSchemas, selectedFields } = contract;
  const output: Record<string, Schema.Json> = {};
  const inspected = yield* inspectRow(topic, "row", row);
  for (const field of selectedFields) {
    const fieldSchema = fieldSchemas.get(field);
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        invalidRow(topic, `Selected row field does not exist for topic ${topic}: ${field}`),
      );
    }
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
    const fieldSchema = fieldSchemas.get(field);
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        invalidRow(topic, `Selected row field does not exist for topic ${topic}: ${field}`),
      );
    }
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

export const encodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  selectedFields: ReadonlySet<string>,
  row: object,
) {
  const contract = compileProjectedRowContract(config.topics[topic]!.schema.fields, selectedFields);
  return yield* encodeProjectedRowWithContract(topic, contract, row);
});

const decodeMaterializedProjectedRow = Effect.fn(
  "ViewServerProtocol.row.project.decodeMaterialized",
)(function* (
  topic: string,
  contract: ViewServerProjectedRowContract,
  row: ViewServerWireRow,
): Effect.fn.Return<object, ViewServerRuntimeError> {
  const { fieldSchemas, selectedFields } = contract;
  const output: Record<string, unknown> = {};
  for (const field of selectedFields) {
    const fieldSchema = fieldSchemas.get(field);
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        invalidRow(topic, `Selected row field does not exist for topic ${topic}: ${field}`),
      );
    }
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
    const fieldSchema = fieldSchemas.get(field);
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        invalidRow(topic, `Selected row field does not exist for topic ${topic}: ${field}`),
      );
    }
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
  const contract = compileProjectedRowContract(config.topics[topic]!.schema.fields, selectedFields);
  return yield* decodeMaterializedProjectedRow(topic, contract, materialized);
});

const encodeGroupedRowWithContract = Effect.fn("ViewServerProtocol.row.grouped.encodeWithContract")(
  function* <const Topics extends TopicDefinitions>(
    config: { readonly topics: Topics },
    topic: Extract<keyof Topics, string>,
    contract: ViewServerCompiledGroupedRowContract,
    row: object,
  ) {
    const { aggregateAliases, aggregates, groupFields, groupFieldSchemas } = contract;
    const output: Record<string, Schema.Json> = {};
    const inspected = yield* inspectRow(topic, "grouped row", row);
    for (const field of groupFields) {
      const fieldSchema = groupFieldSchemas.get(field);
      if (fieldSchema === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Grouped row field does not exist for topic ${topic}: ${field}`),
        );
      }
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
        const fieldSchema = groupFieldSchemas.get(field);
        if (fieldSchema === undefined) {
          return yield* Effect.fail(
            invalidRow(topic, `Grouped row field does not exist for topic ${topic}: ${field}`),
          );
        }
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
  },
);

export const encodeGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  contract: ViewServerGroupedRowContract,
  row: object,
) {
  const compiled = compileGroupedRowContractForTopic(config.topics[topic]!.schema.fields, contract);
  return yield* encodeGroupedRowWithContract(config, topic, compiled, row);
});

const decodeMaterializedGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.decodeMaterialized")(
  function* <const Topics extends TopicDefinitions, Topic extends Extract<keyof Topics, string>>(
    config: { readonly topics: Topics },
    topic: Topic,
    contract: ViewServerCompiledGroupedRowContract,
    row: ViewServerWireRow,
  ): Effect.fn.Return<object, ViewServerRuntimeError> {
    const { aggregateAliases, aggregates, groupFields, groupFieldSchemas } = contract;
    const output: Record<string, unknown> = {};
    for (const field of groupFields) {
      const fieldSchema = groupFieldSchemas.get(field);
      if (fieldSchema === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Grouped row field does not exist for topic ${topic}: ${field}`),
        );
      }
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
        const fieldSchema = groupFieldSchemas.get(field);
        if (fieldSchema === undefined) {
          return yield* Effect.fail(
            invalidRow(topic, `Grouped row field does not exist for topic ${topic}: ${field}`),
          );
        }
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
  },
);

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
  const compiled = compileGroupedRowContractForTopic(config.topics[topic]!.schema.fields, contract);
  return yield* decodeMaterializedGroupedRow(config, topic, compiled, materialized);
});

const compileProjectedEventRowPlan = <ResultRow extends object>(
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: ViewServerEventRawQuery,
): ViewServerEventRowPlan<ResultRow> => {
  const selectedFields = new Set(query.select);
  const contract = compileProjectedRowContract(config.topics[topic]!.schema.fields, selectedFields);
  const proof = compileProjectedRowProof<ResultRow>(contract);
  const proofError = invalidRow(
    topic,
    `Decoded row does not satisfy its compiled contract for topic ${topic}`,
  );
  const proofFailure = Function.constant(proofError);
  const decodeMaterialized: ViewServerEventRowPlan<ResultRow>["decodeMaterialized"] = (row) =>
    decodeMaterializedProjectedRow(topic, contract, row).pipe(
      Effect.filterOrFail(proof, proofFailure),
    );
  const encode: ViewServerEventRowPlan<ResultRow>["encode"] = (row) =>
    encodeProjectedRowWithContract(topic, contract, row);
  return Object.freeze({ decodeMaterialized, encode });
};

const compileGroupedEventRowPlan = <ResultRow extends object>(
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: ViewServerEventGroupedQuery,
): ViewServerEventRowPlan<ResultRow> => {
  const contract = compileGroupedRowContractForTopic(
    config.topics[topic]!.schema.fields,
    compileViewServerGroupedRowContract(query),
  );
  const proof = compileGroupedRowProof<ResultRow>(config.topics[topic]!.schema.fields, contract);
  const proofError = invalidRow(
    topic,
    `Decoded grouped row does not satisfy its compiled contract for topic ${topic}`,
  );
  const proofFailure = Function.constant(proofError);
  const decodeMaterialized: ViewServerEventRowPlan<ResultRow>["decodeMaterialized"] = (row) =>
    decodeMaterializedGroupedRow(config, topic, contract, row).pipe(
      Effect.filterOrFail(proof, proofFailure),
    );
  const encode: ViewServerEventRowPlan<ResultRow>["encode"] = (row) =>
    encodeGroupedRowWithContract(config, topic, contract, row);
  return Object.freeze({ decodeMaterialized, encode });
};

const failedEventRowPlan = <Row extends object>(
  error: ViewServerRuntimeError,
): ViewServerEventRowPlan<Row> =>
  Object.freeze({
    decodeMaterialized: Function.constant(Effect.fail(error)),
    encode: Function.constant(Effect.fail(error)),
  });

const compileRuntimeEventRowPlan = <Row extends object>(
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: ViewServerEventQuery,
): ViewServerEventRowPlan<Row> =>
  Result.match(
    Result.try({
      try: () =>
        isViewServerEventGroupedQuery(query)
          ? compileGroupedEventRowPlan<Row>(config, topic, query)
          : compileProjectedEventRowPlan<Row>(config, topic, query),
      catch: () => invalidRowContract(topic),
    }),
    {
      onFailure: failedEventRowPlan<Row>,
      onSuccess: Function.identity,
    },
  );

export function compileViewServerEventRowPlan<
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
): ViewServerEventRowPlan<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
export function compileViewServerEventRowPlan(
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: ViewServerEventQuery,
): ViewServerEventRowPlan<object> {
  return compileRuntimeEventRowPlan(config, topic, query);
}

export const compileViewServerRuntimeEventRowPlan = (
  config: { readonly topics: TopicDefinitions },
  topic: string,
  query: ViewServerEventQuery,
): ViewServerEventRowPlan<object> => compileRuntimeEventRowPlan(config, topic, query);

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
