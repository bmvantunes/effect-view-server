import { schemaAstChildren } from "@effect-view-server/config/internal";
import {
  compareTrustedWireSafeBigDecimal,
  makeSchemaJsonIdentity,
} from "@effect-view-server/effect-utils";
import { Schema, SchemaAST } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import { compareQueryValue } from "./query-value";

type RowObject = object;
type ValueSchema = Schema.Codec<unknown, unknown, never, never>;
type TopicRowSchema = Schema.Codec<object, unknown, never, never>;
type SchemaValueInput = Schema.Schema.Type<typeof Schema.Unknown>;
const isObjectLike = (value: unknown): value is object =>
  (typeof value === "object" && value !== null) || typeof value === "function";
const topicRowValueSemanticsSchema: unique symbol = Symbol("TopicRowValueSemantics.schema");
const topicRowValueSemanticsSchemas = new WeakMap<object, TopicRowSchema>();

export type SchemaValueSemantics = {
  readonly canonicalKey: (value: SchemaValueInput) => string;
  readonly compare: (left: SchemaValueInput, right: SchemaValueInput) => number;
  readonly decodeEncoded: (value: SchemaValueInput) => unknown;
  readonly equivalent: (left: SchemaValueInput, right: SchemaValueInput) => boolean;
  readonly is: (value: SchemaValueInput) => boolean;
  readonly materialize: (value: SchemaValueInput) => unknown;
  readonly schema: Schema.Codec<unknown, unknown, never, never>;
};

export type TopicRowValueSemantics<Row extends RowObject = RowObject> = {
  readonly [topicRowValueSemanticsSchema]: Schema.Codec<Row, unknown, never, never>;
  readonly equivalentField: (
    field: string,
    left: SchemaValueInput,
    right: SchemaValueInput,
  ) => boolean;
  readonly equivalentRows: <Input extends RowObject>(left: Input, right: Input) => boolean;
  readonly field: (field: string) => SchemaValueSemantics;
  readonly fieldRequired: (field: string) => boolean;
  readonly fieldNames: ReadonlyArray<string>;
  readonly materializeRow: <Input extends RowObject>(row: Input) => RowObject;
  readonly materializeValidatedRowFields: <Input extends RowObject>(row: Input) => RowObject;
};

type SchemaWithFields = TopicRowSchema & {
  readonly fields: Record<string, unknown>;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isSchemaWithFields = (schema: TopicRowSchema): schema is SchemaWithFields =>
  "fields" in schema && isRecord(schema.fields);

const isValueSchema = (value: unknown): value is ValueSchema => Schema.isSchema(value);

const scalarComparable = (value: unknown): boolean =>
  value === null ||
  typeof value === "string" ||
  typeof value === "number" ||
  typeof value === "bigint" ||
  typeof value === "boolean" ||
  value === undefined ||
  isBigDecimal(value);

const isBorrowableImmutablePrimitive = (value: unknown): boolean =>
  value === null ||
  value === undefined ||
  typeof value === "string" ||
  (typeof value === "number" && !Object.is(value, -0)) ||
  typeof value === "bigint" ||
  typeof value === "boolean";

const isBigDecimalAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) &&
  Reflect.get(Object(ast.annotations?.["representation"]), "id") === "effect/schema/BigDecimal";

const schemaContainsBigDecimal = (ast: SchemaAST.AST, seen: Set<SchemaAST.AST>): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  return (
    isBigDecimalAst(ast) ||
    schemaAstChildren(ast).some((child) => schemaContainsBigDecimal(child, seen))
  );
};

const schemaContainsUnorderedEffectCollection = (
  ast: SchemaAST.AST,
  seen: Set<SchemaAST.AST>,
): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  if (SchemaAST.isDeclaration(ast)) {
    const representationId = Reflect.get(Object(ast.annotations?.["representation"]), "id");
    if (
      representationId === "effect/schema/HashMap" ||
      representationId === "effect/schema/HashSet"
    ) {
      return true;
    }
  }
  return schemaAstChildren(ast).some((child) =>
    schemaContainsUnorderedEffectCollection(child, seen),
  );
};

export const makeSchemaValueSemantics = (schema: ValueSchema): SchemaValueSemantics => {
  const identity = makeSchemaJsonIdentity(schema);
  const canonicalObjectKeys = new WeakMap<object, string>();

  const canonicalKey = (value: unknown): string => {
    if (isObjectLike(value)) {
      const cached = canonicalObjectKeys.get(value);
      if (cached !== undefined) {
        return cached;
      }
      const key = identity.canonicalKey(value);
      canonicalObjectKeys.set(value, key);
      return key;
    }
    return identity.canonicalKey(value);
  };

  const schemaEquivalent = Schema.toEquivalence(schema);
  const is = Schema.is(schema);
  const canonicalEquivalenceRequired =
    schemaContainsUnorderedEffectCollection(schema.ast, new Set()) ||
    schemaContainsBigDecimal(schema.ast, new Set());
  const equivalent = canonicalEquivalenceRequired
    ? (left: unknown, right: unknown): boolean => {
        if (Object.is(left, right)) {
          return true;
        }
        if (left === undefined || right === undefined) {
          return false;
        }
        const leftIsBigDecimal = isBigDecimal(left);
        const rightIsBigDecimal = isBigDecimal(right);
        if (leftIsBigDecimal || rightIsBigDecimal) {
          return (
            leftIsBigDecimal &&
            rightIsBigDecimal &&
            compareTrustedWireSafeBigDecimal(left, right) === 0
          );
        }
        return canonicalKey(left) === canonicalKey(right);
      }
    : schemaEquivalent;

  return Object.freeze({
    canonicalKey,
    compare: (left, right) => {
      if (scalarComparable(left) && scalarComparable(right)) {
        return compareQueryValue(left, right);
      }
      const leftKey = canonicalKey(left);
      const rightKey = canonicalKey(right);
      return Number(leftKey > rightKey) - Number(leftKey < rightKey);
    },
    decodeEncoded: identity.decodeEncoded,
    equivalent,
    is,
    materialize: identity.materializeDecoded,
    schema,
  });
};

type TopicRowSchemaSemantics = {
  readonly materialize: <Input extends object>(row: Input) => RowObject;
};

const makeTopicRowSchemaSemantics = (schema: TopicRowSchema): TopicRowSchemaSemantics => {
  const identity = makeSchemaJsonIdentity<RowObject>(schema);
  return {
    materialize: identity.materializeDecoded,
  };
};

const unknownValueSemantics = makeSchemaValueSemantics(Schema.Unknown);

const schemaFieldSemantics = (
  schema: TopicRowSchema,
): ReadonlyMap<string, SchemaValueSemantics> => {
  if (!isSchemaWithFields(schema)) {
    return new Map();
  }
  const fields = new Map<string, SchemaValueSemantics>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    fields.set(
      field,
      isValueSchema(fieldSchema) ? makeSchemaValueSemantics(fieldSchema) : unknownValueSemantics,
    );
  }
  return fields;
};

const validateRowFieldDescriptors = <Row extends RowObject>(
  row: Row,
  fieldNames: ReadonlyArray<string>,
): void => {
  for (const field of fieldNames) {
    const descriptor = Object.getOwnPropertyDescriptor(row, field);
    if (descriptor === undefined) {
      continue;
    }
    if (!("value" in descriptor)) {
      throw new TypeError(`Topic Row field ${field} must be an own data property.`);
    }
  }
};

export const makeTopicRowValueSemantics = <SchemaValue extends TopicRowSchema>(
  schema: SchemaValue,
): TopicRowValueSemantics<SchemaValue["Type"]> => {
  const fields = schemaFieldSemantics(schema);
  const fieldNames = Object.freeze([...fields.keys()]);
  let cachedRowSemantics: TopicRowSchemaSemantics | undefined;
  const rowSemantics = (): TopicRowSchemaSemantics => {
    cachedRowSemantics ??= makeTopicRowSchemaSemantics(schema);
    return cachedRowSemantics;
  };
  const field = (name: string): SchemaValueSemantics => {
    const semantics = fields.get(name);
    if (semantics === undefined) {
      throw new TypeError(`Unknown Topic Row field: ${name}.`);
    }
    return semantics;
  };

  const semantics: TopicRowValueSemantics<SchemaValue["Type"]> = Object.freeze({
    [topicRowValueSemanticsSchema]: schema,
    equivalentField: (name, left, right) => field(name).equivalent(left, right),
    equivalentRows: <Input extends RowObject>(left: Input, right: Input) => {
      for (const name of fieldNames) {
        const leftHasField = Object.prototype.propertyIsEnumerable.call(left, name);
        if (leftHasField !== Object.prototype.propertyIsEnumerable.call(right, name)) {
          return false;
        }
        if (
          leftHasField &&
          !field(name).equivalent(Reflect.get(left, name), Reflect.get(right, name))
        ) {
          return false;
        }
      }
      return true;
    },
    field,
    fieldRequired: (name) => !SchemaAST.isOptional(field(name).schema.ast),
    fieldNames,
    materializeRow: <Input extends RowObject>(row: Input) => {
      validateRowFieldDescriptors(row, fieldNames);
      return rowSemantics().materialize(row);
    },
    materializeValidatedRowFields: <Input extends RowObject>(row: Input) => {
      validateRowFieldDescriptors(row, fieldNames);
      for (const name of fieldNames) {
        if (!Object.prototype.propertyIsEnumerable.call(row, name)) {
          continue;
        }
        const value = Reflect.get(row, name);
        if (isBorrowableImmutablePrimitive(value)) {
          continue;
        }
        Object.defineProperty(row, name, {
          configurable: true,
          enumerable: true,
          value: field(name).materialize(value),
          writable: true,
        });
      }
      return row;
    },
  });
  topicRowValueSemanticsSchemas.set(semantics, schema);
  return semantics;
};

export const topicRowValueSemanticsMatchesSchema = <SchemaValue extends TopicRowSchema>(
  semantics: TopicRowValueSemantics,
  schema: SchemaValue,
): semantics is TopicRowValueSemantics<SchemaValue["Type"]> =>
  semantics[topicRowValueSemanticsSchema] === schema &&
  topicRowValueSemanticsSchemas.get(semantics) === schema;

export const topicRowValueSemanticsShareSchema = (
  left: TopicRowValueSemantics,
  right: TopicRowValueSemantics,
): boolean => {
  const schema = topicRowValueSemanticsSchemas.get(left);
  return schema !== undefined && topicRowValueSemanticsSchemas.get(right) === schema;
};
