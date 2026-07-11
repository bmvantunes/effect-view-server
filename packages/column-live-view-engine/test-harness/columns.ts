import { rawQueryCompilerMetadata } from "../src/raw-query-compiler";
import { fieldValue } from "../src/row-values";
import {
  createTopicColumnValuesFromArray,
  type TopicColumnValues,
} from "../src/topic-column-vector";

export const makeColumns = (
  metadata: ReturnType<typeof rawQueryCompilerMetadata>,
  entries: ReadonlyArray<readonly [string, ReadonlyArray<unknown>]>,
): Map<string, TopicColumnValues> => {
  const columns = new Map<string, TopicColumnValues>();
  for (const [field, values] of entries) {
    columns.set(field, createTopicColumnValuesFromArray(field, metadata, values));
  }
  return columns;
};

export const numericRowField = (row: object, field: string): number => {
  const value = fieldValue(row, field);
  if (typeof value === "number") {
    return value;
  }
  throw new Error(`Expected numeric row field ${field}.`);
};
