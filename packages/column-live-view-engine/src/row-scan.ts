import { fieldValue } from "./row-values";

type RowObject = object;

const changedFieldsTypeId = Symbol("view-server/TopicRowChangedFields");

export type TopicRowChangedFields = {
  readonly fields: ReadonlySet<string>;
  readonly [changedFieldsTypeId]: typeof changedFieldsTypeId;
};

export type TopicRowEntry<Row extends RowObject> = {
  readonly key: string;
  readonly row: Row;
};

export type TopicRowVisitor<Row extends RowObject> = (key: string, row: Row) => false | void;

export type TopicRowChange<Row extends RowObject> = {
  readonly changedFields?: TopicRowChangedFields;
  readonly key: string;
  readonly next: Row | undefined;
  readonly previous: Row | undefined;
};

function makeTopicRowChangedFields(fields: Iterable<string>): TopicRowChangedFields | undefined {
  const changedFields = new Set(fields);
  if (changedFields.size === 0) {
    return undefined;
  }
  return {
    [changedFieldsTypeId]: changedFieldsTypeId,
    fields: changedFields,
  };
}

export const isTopicRowChangedFields = (value: unknown): value is TopicRowChangedFields =>
  typeof value === "object" && value !== null && Object.hasOwn(value, changedFieldsTypeId);

export const topicRowChangedFieldsFromRows = (
  previous: RowObject,
  next: RowObject,
  fields: Iterable<string>,
  equivalent: (field: string, left: unknown, right: unknown) => boolean,
): TopicRowChangedFields | undefined => {
  const changedFields = new Set<string>();
  for (const field of fields) {
    if (
      Object.prototype.propertyIsEnumerable.call(previous, field) !==
        Object.prototype.propertyIsEnumerable.call(next, field) ||
      !equivalent(field, fieldValue(previous, field), fieldValue(next, field))
    ) {
      changedFields.add(field);
    }
  }
  return makeTopicRowChangedFields(changedFields);
};

export type TopicRowChangeBatch<Row extends RowObject> = {
  readonly changes: ReadonlyArray<TopicRowChange<Row>>;
  readonly version: number;
};

export type TopicRowScan<Row extends RowObject> = {
  readonly changesSince: (version: number) => ReadonlyArray<TopicRowChangeBatch<Row>> | undefined;
  readonly scanRows: (visitor: TopicRowVisitor<Row>) => void;
  readonly version: () => number;
};
