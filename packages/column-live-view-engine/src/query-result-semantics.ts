import { Schema } from "effect";
import {
  makeSchemaValueSemantics,
  type SchemaValueSemantics,
  type TopicRowValueSemantics,
} from "./topic-row-value-semantics";

type RowObject = object;

export type QueryResultFieldSemantics = {
  readonly field: string;
  readonly semantics: SchemaValueSemantics;
};

export type QueryResultSemantics = {
  readonly equivalentRows: (left: RowObject, right: RowObject) => boolean;
  readonly materializeRow: <Row extends RowObject>(row: Row) => Row;
  readonly projectRow: (row: RowObject) => RowObject;
};

const defineResultField = (row: Record<string, unknown>, field: string, value: unknown): void => {
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
  typeof value === "number" ||
  typeof value === "bigint" ||
  typeof value === "boolean";

const materializeValue = (semantics: SchemaValueSemantics, value: unknown): unknown =>
  isBorrowableImmutablePrimitive(value) ? value : semantics.materialize(value);

export const makeQueryResultSemantics = (
  fields: ReadonlyArray<QueryResultFieldSemantics>,
): QueryResultSemantics => {
  const projectRow = (
    row: RowObject,
    projectValue: (semantics: SchemaValueSemantics, value: unknown) => unknown,
  ): RowObject => {
    const projected: Record<string, unknown> = {};
    for (const { field, semantics } of fields) {
      if (hasEnumerableField(row, field)) {
        defineResultField(projected, field, projectValue(semantics, Reflect.get(row, field)));
      }
    }
    return projected;
  };

  function materializeRow<Row extends RowObject>(row: Row): Row;
  function materializeRow(row: RowObject): RowObject {
    return projectRow(row, materializeValue);
  }

  return {
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
    materializeRow,
    projectRow: (row) => projectRow(row, borrowValue),
  };
};

export const rawQueryResultSemantics = (
  topicRow: TopicRowValueSemantics,
  selectedFields: ReadonlyArray<string>,
): QueryResultSemantics =>
  makeQueryResultSemantics(
    selectedFields.map((field) => ({
      field,
      semantics: topicRow.field(field),
    })),
  );

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
): SchemaValueSemantics => ({
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
  materialize: (value) => (value === undefined ? undefined : semantics.materialize(value)),
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
  return allowUndefinedResultValueSemantics(topicRow.field(aggregate.field));
};

export const groupedQueryResultSemantics = (
  topicRow: TopicRowValueSemantics,
  groupBy: ReadonlyArray<string>,
  aggregateFields: ReadonlyArray<GroupedResultFieldSemantics>,
): QueryResultSemantics =>
  makeQueryResultSemantics([
    ...groupBy.map((field) => ({
      field,
      semantics: topicRow.field(field),
    })),
    ...aggregateFields.map(({ alias, resultSemantics }) => ({
      field: alias,
      semantics: resultSemantics,
    })),
  ]);
