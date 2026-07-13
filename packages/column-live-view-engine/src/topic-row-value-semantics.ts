import { schemaAstChildren } from "@effect-view-server/config/internal";
import { makeSchemaJsonIdentity } from "@effect-view-server/effect-utils";
import { Schema, SchemaAST } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import { compareQueryValue } from "./query-value";

type RowObject = object;
type ValueSchema = Schema.Codec<unknown, unknown, never, never>;
type TopicRowSchema = Schema.Codec<object, unknown, never, never>;

export type SchemaValueSemantics = {
  readonly canonicalKey: (value: unknown) => string;
  readonly compare: (left: unknown, right: unknown) => number;
  readonly decodeEncoded: (value: unknown) => unknown;
  readonly equivalent: (left: unknown, right: unknown) => boolean;
  readonly materialize: (value: unknown) => unknown;
};

export type TopicRowValueSemantics = {
  readonly equivalentField: (field: string, left: unknown, right: unknown) => boolean;
  readonly equivalentRows: (left: RowObject, right: RowObject) => boolean;
  readonly field: (field: string) => SchemaValueSemantics;
  readonly fieldNames: ReadonlyArray<string>;
  readonly materializeRow: (row: RowObject) => RowObject;
  readonly materializeValidatedRowFields: (row: RowObject) => RowObject;
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

const unorderedEffectCollectionTags = new Set(["effect/HashMap", "effect/HashSet"]);

const schemaContainsUnorderedEffectCollection = (
  ast: SchemaAST.AST,
  seen: Set<SchemaAST.AST>,
): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  if (SchemaAST.isDeclaration(ast)) {
    const typeConstructor = ast.annotations?.["typeConstructor"];
    const tag = Reflect.get(Object(typeConstructor), "_tag");
    if (typeof tag === "string" && unorderedEffectCollectionTags.has(tag)) {
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
    if ((typeof value === "object" && value !== null) || typeof value === "function") {
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
  const equivalent = schemaContainsUnorderedEffectCollection(schema.ast, new Set())
    ? (left: unknown, right: unknown): boolean => canonicalKey(left) === canonicalKey(right)
    : schemaEquivalent;

  return {
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
    materialize: identity.materializeDecoded,
  };
};

type TopicRowSchemaSemantics = {
  readonly materialize: (row: RowObject) => RowObject;
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

const validateRowFieldDescriptors = (row: RowObject, fieldNames: ReadonlyArray<string>): void => {
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

export const makeTopicRowValueSemantics = (schema: TopicRowSchema): TopicRowValueSemantics => {
  const fields = schemaFieldSemantics(schema);
  const fieldNames = [...fields.keys()];
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

  return {
    equivalentField: (name, left, right) => field(name).equivalent(left, right),
    equivalentRows: (left, right) => {
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
    fieldNames,
    materializeRow: (row) => {
      validateRowFieldDescriptors(row, fieldNames);
      return rowSemantics().materialize(row);
    },
    materializeValidatedRowFields: (row) => {
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
  };
};
