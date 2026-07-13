import type {
  GroupedQuery,
  GroupedResult,
  PickRawFields,
  RawQuery,
} from "@effect-view-server/config";
import { Schema } from "effect";
import {
  typedRuntimeGroupedQueryMatchesSemantics,
  type TypedRuntimeGroupedQuery,
} from "./grouped-query-decoder";
import {
  typedRuntimeRawQueryMatchesSemantics,
  type TypedRuntimeRawQuery,
} from "./raw-query-decoder";
import {
  makeSchemaValueSemantics,
  type SchemaValueSemantics,
  type TopicRowValueSemantics,
  topicRowValueSemanticsShareSchema,
} from "./topic-row-value-semantics";
import {
  consumeTopicStorageResultProjection,
  type TopicStorageResultProjection,
} from "./topic-row-storage";

type RowObject = object;

export type QueryResultFieldSemantics = {
  readonly field: string;
  readonly required?: boolean;
  readonly semantics: SchemaValueSemantics;
};

type QueryResultProjectValue = (semantics: SchemaValueSemantics, value: unknown) => unknown;

type QueryResultProof<ResultRow extends RowObject> = (row: RowObject) => row is ResultRow;

const queryResultTopicStorageProjectionProofBrand: unique symbol = Symbol(
  "QueryResultTopicStorageProjectionProof",
);
const queryResultTopicStorageProjectionProofConstructionToken = Object.freeze({});

class QueryResultTopicStorageProjectionProofMarker<ResultRow extends RowObject> {
  declare private readonly output: ResultRow;
}

export type QueryResultTopicStorageProjectionProof<ResultRow extends RowObject> = {
  readonly [queryResultTopicStorageProjectionProofBrand]: QueryResultTopicStorageProjectionProofMarker<ResultRow>;
  readonly matchesValueSemantics: (valueSemantics: TopicRowValueSemantics) => boolean;
  readonly selectedFields: ReadonlyArray<string>;
};

class TopicStorageResultProjectionProof<ResultRow extends RowObject> {
  readonly [queryResultTopicStorageProjectionProofBrand] =
    new QueryResultTopicStorageProjectionProofMarker<ResultRow>();
  readonly selectedFields: ReadonlyArray<string>;

  constructor(
    constructionToken: object,
    private readonly topicRow: TopicRowValueSemantics,
    fields: ReadonlyArray<QueryResultFieldSemantics>,
  ) {
    if (constructionToken !== queryResultTopicStorageProjectionProofConstructionToken) {
      throw new TypeError("Query Result Topic Storage projection proof construction is private.");
    }
    this.selectedFields = Object.freeze(fields.map(({ field }) => field));
    Object.freeze(this);
  }

  matchesValueSemantics(valueSemantics: TopicRowValueSemantics): boolean {
    return topicRowValueSemanticsShareSchema(valueSemantics, this.topicRow);
  }

  project(projection: TopicStorageResultProjection): ResultRow {
    const row = consumeTopicStorageResultProjection(projection, this);
    // The concrete Topic Row Storage capability has already proven schema
    // identity, token-owned selected fields, and required-field presence.
    const authenticate: (value: RowObject) => asserts value is ResultRow = () => {};
    authenticate(row);
    return row;
  }
}

Object.freeze(TopicStorageResultProjectionProof.prototype);

export type QueryResultSemantics<ResultRow extends RowObject = RowObject> = {
  readonly equivalentRows: (left: RowObject, right: RowObject) => boolean;
  readonly materializeOwnedRow: (row: RowObject) => ResultRow;
  readonly materializeRow: (row: RowObject) => ResultRow;
  readonly narrowProjectedRow: (row: RowObject) => ResultRow;
  readonly projectRow: (row: RowObject) => ResultRow;
};

export type TopicStorageProjectableQueryResultSemantics<ResultRow extends RowObject = RowObject> =
  QueryResultSemantics<ResultRow> & {
    readonly projectTopicStorageRow: (projection: TopicStorageResultProjection) => ResultRow;
    readonly topicStorageProjectionProof: QueryResultTopicStorageProjectionProof<ResultRow>;
  };

const defineResultField = (row: RowObject, field: string, value: unknown): void => {
  Object.defineProperty(row, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
};

const hasEnumerableField = (row: RowObject, field: string): boolean =>
  Object.prototype.propertyIsEnumerable.call(row, field);

const borrowValue = (_semantics: SchemaValueSemantics, value: unknown): unknown => value;

const isBorrowableImmutablePrimitive = (value: unknown): boolean =>
  value === null ||
  value === undefined ||
  typeof value === "string" ||
  (typeof value === "number" && !Object.is(value, -0)) ||
  typeof value === "bigint" ||
  typeof value === "boolean";

const materializeValue = (semantics: SchemaValueSemantics, value: unknown): unknown =>
  isBorrowableImmutablePrimitive(value) ? value : semantics.materialize(value);

const projectFields = (
  fields: ReadonlyArray<QueryResultFieldSemantics>,
  row: RowObject,
  projectValue: QueryResultProjectValue,
): RowObject => {
  const projected: Record<string, unknown> = {};
  for (const { field, semantics } of fields) {
    if (hasEnumerableField(row, field)) {
      defineResultField(projected, field, projectValue(semantics, Reflect.get(row, field)));
    }
  }
  return projected;
};

const projectedFieldsCheck = (
  fields: ReadonlyArray<QueryResultFieldSemantics>,
  validateValues: boolean,
): ((row: RowObject) => boolean) => {
  const expectedFields = new Set(fields.map(({ field }) => field));
  return (row) => {
    for (const field of Object.keys(row)) {
      if (!expectedFields.has(field)) {
        return false;
      }
    }
    for (const { field, required, semantics } of fields) {
      const descriptor = Object.getOwnPropertyDescriptor(row, field);
      if (descriptor === undefined) {
        if (required === true) {
          return false;
        }
        continue;
      }
      if (!("value" in descriptor) || !descriptor.enumerable) {
        return false;
      }
      if (validateValues && !semantics.is(descriptor.value)) {
        return false;
      }
    }
    return true;
  };
};

const constructedProjectionProof = <ResultRow extends RowObject>(
  fields: ReadonlyArray<QueryResultFieldSemantics>,
  validateValues: boolean,
): QueryResultProof<ResultRow> => {
  return (row): row is ResultRow => {
    for (const { field, required, semantics } of fields) {
      if (!hasEnumerableField(row, field)) {
        if (required === true) {
          return false;
        }
        continue;
      }
      if (validateValues && !semantics.is(Reflect.get(row, field))) {
        return false;
      }
    }
    return true;
  };
};

const untypedResultProof = (
  fields: ReadonlyArray<QueryResultFieldSemantics>,
): QueryResultProof<RowObject> => {
  const check = projectedFieldsCheck(fields, false);
  return (row): row is RowObject => check(row);
};

const rawResultProof = <Row extends RowObject, const Query extends RawQuery<Row>>(
  fields: ReadonlyArray<QueryResultFieldSemantics>,
): QueryResultProof<PickRawFields<Row, Query>> => {
  const check = projectedFieldsCheck(fields, true);
  return (row): row is PickRawFields<Row, Query> => check(row);
};

const groupedResultProof = <Row extends RowObject, const Query extends GroupedQuery<Row>>(
  fields: ReadonlyArray<QueryResultFieldSemantics>,
): QueryResultProof<GroupedResult<Row, Query>> => {
  const check = projectedFieldsCheck(fields, true);
  return (row): row is GroupedResult<Row, Query> => check(row);
};

const runtimeResultProof = (
  fields: ReadonlyArray<QueryResultFieldSemantics>,
): QueryResultProof<RowObject> => {
  const check = projectedFieldsCheck(fields, true);
  return (row): row is RowObject => check(row);
};

const makeProjectedQueryResultSemantics = <ResultRow extends RowObject>(
  fields: ReadonlyArray<QueryResultFieldSemantics>,
  isResultRow: QueryResultProof<ResultRow>,
  validateProjectedValues: boolean,
): QueryResultSemantics<ResultRow> => {
  const narrowProjectedRow = (row: RowObject): ResultRow => {
    if (!isResultRow(row)) {
      throw new TypeError("Projected Query Result Row does not satisfy its compiled proof.");
    }
    return row;
  };
  const isConstructedProjection = constructedProjectionProof<ResultRow>(
    fields,
    validateProjectedValues,
  );
  const narrowConstructedProjection = (row: RowObject): ResultRow => {
    if (!isConstructedProjection(row)) {
      throw new TypeError("Projected Query Result Row does not satisfy its compiled proof.");
    }
    return row;
  };
  return Object.freeze({
    equivalentRows: (left, right) => {
      for (const { field, semantics } of fields) {
        const leftHasField = hasEnumerableField(left, field);
        if (leftHasField !== hasEnumerableField(right, field)) {
          return false;
        }
        if (
          leftHasField &&
          !semantics.equivalent(Reflect.get(left, field), Reflect.get(right, field))
        ) {
          return false;
        }
      }
      return true;
    },
    materializeOwnedRow: (row) => {
      for (const { field, semantics } of fields) {
        if (!hasEnumerableField(row, field)) {
          continue;
        }
        const value = Reflect.get(row, field);
        if (isBorrowableImmutablePrimitive(value)) {
          continue;
        }
        defineResultField(row, field, semantics.materialize(value));
      }
      return narrowProjectedRow(row);
    },
    materializeRow: (row) => narrowProjectedRow(projectFields(fields, row, materializeValue)),
    narrowProjectedRow,
    projectRow: (row) => narrowConstructedProjection(projectFields(fields, row, borrowValue)),
  });
};

const makeTopicStorageProjectableQueryResultSemantics = <ResultRow extends RowObject>(
  fields: ReadonlyArray<QueryResultFieldSemantics>,
  isResultRow: QueryResultProof<ResultRow>,
  validateProjectedValues: boolean,
  topicRow: TopicRowValueSemantics,
): TopicStorageProjectableQueryResultSemantics<ResultRow> => {
  const semantics = makeProjectedQueryResultSemantics(fields, isResultRow, validateProjectedValues);
  const topicStorageProjectionProof = new TopicStorageResultProjectionProof<ResultRow>(
    queryResultTopicStorageProjectionProofConstructionToken,
    topicRow,
    fields,
  );
  return Object.freeze({
    ...semantics,
    projectTopicStorageRow: (projection) => topicStorageProjectionProof.project(projection),
    topicStorageProjectionProof,
  });
};

export const makeQueryResultSemantics = (
  fields: ReadonlyArray<QueryResultFieldSemantics>,
): QueryResultSemantics<RowObject> =>
  makeProjectedQueryResultSemantics(fields, untypedResultProof(fields), false);

const rawResultFields = (
  topicRow: TopicRowValueSemantics,
  selectedFields: ReadonlyArray<string>,
): ReadonlyArray<QueryResultFieldSemantics> =>
  selectedFields.map((field) => ({
    field,
    required: topicRow.fieldRequired(field),
    semantics: topicRow.field(field),
  }));

export const rawQueryResultSemantics = <Row extends RowObject, const Query extends RawQuery<Row>>(
  topicRow: TopicRowValueSemantics<Row>,
  query: TypedRuntimeRawQuery<NoInfer<Row>, Query>,
): TopicStorageProjectableQueryResultSemantics<PickRawFields<Row, Query>> => {
  if (!typedRuntimeRawQueryMatchesSemantics(query, topicRow)) {
    throw new TypeError("Typed raw query proof does not match its Topic Row Value Semantics.");
  }
  const fields = rawResultFields(topicRow, query.select);
  return makeTopicStorageProjectableQueryResultSemantics(
    fields,
    rawResultProof<Row, Query>(fields),
    true,
    topicRow,
  );
};

export const runtimeRawQueryResultSemantics = (
  topicRow: TopicRowValueSemantics,
  selectedFields: ReadonlyArray<string>,
): TopicStorageProjectableQueryResultSemantics<RowObject> => {
  const fields = rawResultFields(topicRow, selectedFields);
  return makeTopicStorageProjectableQueryResultSemantics(
    fields,
    runtimeResultProof(fields),
    true,
    topicRow,
  );
};

const countSemantics = makeSchemaValueSemantics(Schema.BigInt);
const bigDecimalSemantics = makeSchemaValueSemantics(Schema.BigDecimal);

export type GroupedResultAggregate =
  | {
      readonly aggFunc: "count";
    }
  | {
      readonly aggFunc: "countDistinct" | "min" | "max" | "avg";
      readonly field: string;
    }
  | {
      readonly aggFunc: "sum";
      readonly field: string;
      readonly resultKind: "bigint" | "bigDecimal";
    };

export type GroupedResultFieldSemantics = {
  readonly alias: string;
  readonly resultSemantics: SchemaValueSemantics;
};

const allowUndefinedResultValueSemantics = (
  semantics: SchemaValueSemantics,
): SchemaValueSemantics =>
  Object.freeze({
    canonicalKey: (value) =>
      value === undefined ? "undefined:" : `value:${semantics.canonicalKey(value)}`,
    compare: (left, right) => {
      if (left === undefined) {
        return right === undefined ? 0 : -1;
      }
      if (right === undefined) {
        return 1;
      }
      return semantics.compare(left, right);
    },
    decodeEncoded: (value) => (value === undefined ? undefined : semantics.decodeEncoded(value)),
    equivalent: (left, right) => {
      if (left === undefined) {
        return right === undefined;
      }
      return right !== undefined && semantics.equivalent(left, right);
    },
    is: (value) => value === undefined || semantics.is(value),
    materialize: (value) => (value === undefined ? undefined : semantics.materialize(value)),
    schema: Schema.UndefinedOr(semantics.schema),
  });

export const groupedResultAggregateSemantics = (
  topicRow: TopicRowValueSemantics,
  aggregate: GroupedResultAggregate,
): SchemaValueSemantics => {
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return countSemantics;
  }
  if (aggregate.aggFunc === "avg") {
    return bigDecimalSemantics;
  }
  if (aggregate.aggFunc === "sum") {
    return aggregate.resultKind === "bigint" ? countSemantics : bigDecimalSemantics;
  }
  const fieldSemantics = topicRow.field(aggregate.field);
  return topicRow.fieldRequired(aggregate.field)
    ? fieldSemantics
    : allowUndefinedResultValueSemantics(fieldSemantics);
};

const groupedResultFields = (
  topicRow: TopicRowValueSemantics,
  groupBy: ReadonlyArray<string>,
  aggregateFields: ReadonlyArray<GroupedResultFieldSemantics>,
): ReadonlyArray<QueryResultFieldSemantics> => [
  ...groupBy.map((field) => ({
    field,
    required: topicRow.fieldRequired(field),
    semantics: topicRow.field(field),
  })),
  ...aggregateFields.map(({ alias, resultSemantics }) => ({
    field: alias,
    required: true,
    semantics: resultSemantics,
  })),
];

export const groupedQueryResultSemantics = <
  Row extends RowObject,
  const Query extends GroupedQuery<Row>,
>(
  topicRow: TopicRowValueSemantics<Row>,
  query: TypedRuntimeGroupedQuery<NoInfer<Row>, Query>,
): QueryResultSemantics<GroupedResult<Row, Query>> => {
  if (!typedRuntimeGroupedQueryMatchesSemantics(query, topicRow)) {
    throw new TypeError("Typed grouped query proof does not match its Topic Row Value Semantics.");
  }
  const aggregateFields = Object.entries(query.aggregates).map(([alias, aggregate]) => ({
    alias,
    resultSemantics: groupedResultAggregateSemantics(topicRow, aggregate),
  }));
  const fields = groupedResultFields(topicRow, query.groupBy, aggregateFields);
  return makeProjectedQueryResultSemantics(fields, groupedResultProof<Row, Query>(fields), true);
};

export const runtimeGroupedQueryResultSemantics = (
  topicRow: TopicRowValueSemantics,
  groupBy: ReadonlyArray<string>,
  aggregateFields: ReadonlyArray<GroupedResultFieldSemantics>,
): QueryResultSemantics<RowObject> => {
  const fields = groupedResultFields(topicRow, groupBy, aggregateFields);
  return makeProjectedQueryResultSemantics(fields, runtimeResultProof(fields), true);
};
