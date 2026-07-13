import { viewServerSchemaFieldMetadata } from "@effect-view-server/config";
import { Schema, SchemaAST } from "effect";
import { immutableReadonlyMap, immutableReadonlySet } from "./immutable-readonly-collection";
import { isRecord } from "./row-values";
import {
  makeTopicRowValueSemantics,
  topicRowValueSemanticsMatchesSchema,
  type TopicRowValueSemantics,
} from "./topic-row-value-semantics";

type SchemaWithFields = Schema.Codec<object, unknown, never, never> & {
  readonly fields: Record<string, unknown>;
};
const rawQueryCompilerMetadataSchema: unique symbol = Symbol("RawQueryCompilerMetadata.schema");
const rawQueryCompilerMetadataSchemas = new WeakMap<
  object,
  Schema.Codec<object, unknown, never, never>
>();

export type RangeValueKind = "number" | "bigint" | "bigDecimal";

export type RawQueryCompilerMetadata<Row extends object = object> = {
  readonly [rawQueryCompilerMetadataSchema]: Schema.Codec<Row, unknown, never, never>;
  readonly schema: Schema.Codec<Row, unknown, never, never>;
  readonly fieldNames: ReadonlySet<string>;
  readonly fieldOrder: ReadonlyArray<string>;
  readonly fieldMetadata: ReadonlyMap<string, ReturnType<typeof viewServerSchemaFieldMetadata>>;
  readonly structuredFieldNames: ReadonlySet<string>;
  readonly structuredObjectFieldNames: ReadonlySet<string>;
  readonly stringFieldNames: ReadonlySet<string>;
  readonly numericFieldNames: ReadonlySet<string>;
  readonly numberFieldNames: ReadonlySet<string>;
  readonly bigintFieldNames: ReadonlySet<string>;
  readonly bigDecimalFieldNames: ReadonlySet<string>;
  readonly exactScalarEqualityFieldNames: ReadonlySet<string>;
  readonly rangeValueKinds: ReadonlyMap<string, ReadonlySet<RangeValueKind>>;
  readonly valueSemantics: TopicRowValueSemantics<Row>;
};

const isSchemaWithFields = (
  schema: Schema.Codec<object, unknown, never, never>,
): schema is SchemaWithFields => "fields" in schema && isRecord(schema.fields);

const schemaAst = (schema: unknown): SchemaAST.AST | undefined => {
  if (!isRecord(schema)) {
    return undefined;
  }
  const ast = schema["ast"];
  return SchemaAST.isAST(ast) ? ast : undefined;
};

const isBigDecimalAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) &&
  isRecord(ast.annotations?.["typeConstructor"]) &&
  ast.annotations["typeConstructor"]["_tag"] === "effect/BigDecimal";

const builtInBigDecimalEquivalence = SchemaAST.resolve(Schema.BigDecimal.ast)?.["toEquivalence"];

const astHasExactScalarEquality = (ast: SchemaAST.AST): boolean => {
  const equivalence = SchemaAST.resolve(ast)?.["toEquivalence"];
  if (equivalence !== undefined) {
    return isBigDecimalAst(ast) && equivalence === builtInBigDecimalEquivalence;
  }
  if (SchemaAST.isUnion(ast)) {
    return ast.types.length > 0 && ast.types.every((member) => astHasExactScalarEquality(member));
  }
  if (SchemaAST.isSuspend(ast)) {
    return false;
  }
  return !SchemaAST.isArrays(ast) && !SchemaAST.isObjects(ast);
};

const rangeValueKindsAst = (ast: SchemaAST.AST): ReadonlySet<RangeValueKind> => {
  if (SchemaAST.isNumber(ast)) {
    return new Set(["number"]);
  }
  if (SchemaAST.isBigInt(ast)) {
    return new Set(["bigint"]);
  }
  if (isBigDecimalAst(ast)) {
    return new Set(["bigDecimal"]);
  }
  if (SchemaAST.isLiteral(ast)) {
    if (typeof ast.literal === "number") {
      return new Set(["number"]);
    }
    if (typeof ast.literal === "bigint") {
      return new Set(["bigint"]);
    }
    return new Set();
  }
  if (!SchemaAST.isUnion(ast) || ast.types.length === 0) {
    return new Set();
  }
  const kinds = new Set<RangeValueKind>();
  for (const member of ast.types) {
    for (const kind of rangeValueKindsAst(member)) {
      kinds.add(kind);
    }
  }
  return kinds;
};

const isPureNumberAst = (ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isNumber(ast)) {
    return true;
  }
  if (SchemaAST.isLiteral(ast)) {
    return typeof ast.literal === "number";
  }
  return SchemaAST.isUnion(ast) && ast.types.length > 0 && ast.types.every(isPureNumberAst);
};

const isPureBigDecimalAst = (ast: SchemaAST.AST): boolean => {
  if (isBigDecimalAst(ast)) {
    return true;
  }
  return SchemaAST.isUnion(ast) && ast.types.length > 0 && ast.types.every(isPureBigDecimalAst);
};

const schemaFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> =>
  isSchemaWithFields(schema) ? new Set(Object.keys(schema.fields)) : new Set();

const schemaFieldOrder = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlyArray<string> => (isSchemaWithFields(schema) ? Object.keys(schema.fields) : []);

const schemaFieldMetadata = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlyMap<string, ReturnType<typeof viewServerSchemaFieldMetadata>> => {
  if (!isSchemaWithFields(schema)) {
    return new Map();
  }

  const fields = new Map<string, ReturnType<typeof viewServerSchemaFieldMetadata>>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    fields.set(field, Object.freeze(viewServerSchemaFieldMetadata(fieldSchema)));
  }
  return fields;
};

const schemaNumericFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (!viewServerSchemaFieldMetadata(fieldSchema).isNumeric) {
      continue;
    }
    fields.add(field);
  }
  return fields;
};

const schemaNumberFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const ast = schemaAst(fieldSchema);
    if (ast === undefined || !isPureNumberAst(ast)) {
      continue;
    }
    fields.add(field);
  }
  return fields;
};

const schemaBigintFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isPureBigInt) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaBigDecimalFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const ast = schemaAst(fieldSchema);
    if (ast === undefined || !isPureBigDecimalAst(ast)) {
      continue;
    }
    fields.add(field);
  }
  return fields;
};

const schemaExactScalarEqualityFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const ast = schemaAst(fieldSchema);
    if (ast !== undefined && astHasExactScalarEquality(ast)) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaRangeValueKinds = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlyMap<string, ReadonlySet<RangeValueKind>> => {
  if (!isSchemaWithFields(schema)) {
    return new Map();
  }

  const fields = new Map<string, ReadonlySet<RangeValueKind>>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const ast = schemaAst(fieldSchema);
    if (ast === undefined) {
      continue;
    }
    const kinds = rangeValueKindsAst(ast);
    if (kinds.size > 0) {
      fields.set(field, kinds);
    }
  }
  return fields;
};

const schemaStringFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isString) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaStructuredFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isStructured) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaStructuredObjectFieldNames = (
  schema: Schema.Codec<object, unknown, never, never>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isStructuredObject) {
      fields.add(field);
    }
  }
  return fields;
};

const immutableRangeValueKinds = (
  fields: ReadonlyMap<string, ReadonlySet<RangeValueKind>>,
): ReadonlyMap<string, ReadonlySet<RangeValueKind>> => {
  const entries: Array<readonly [string, ReadonlySet<RangeValueKind>]> = [];
  for (const [field, kinds] of fields) {
    entries.push([field, immutableReadonlySet(kinds)]);
  }
  return immutableReadonlyMap(entries);
};

export const rawQueryCompilerMetadata = <
  SchemaValue extends Schema.Codec<object, unknown, never, never>,
>(
  schema: SchemaValue,
): RawQueryCompilerMetadata<SchemaValue["Type"]> => {
  const metadata: RawQueryCompilerMetadata<SchemaValue["Type"]> = Object.freeze({
    [rawQueryCompilerMetadataSchema]: schema,
    schema,
    fieldNames: immutableReadonlySet(schemaFieldNames(schema)),
    fieldOrder: Object.freeze([...schemaFieldOrder(schema)]),
    fieldMetadata: immutableReadonlyMap(schemaFieldMetadata(schema)),
    structuredFieldNames: immutableReadonlySet(schemaStructuredFieldNames(schema)),
    structuredObjectFieldNames: immutableReadonlySet(schemaStructuredObjectFieldNames(schema)),
    stringFieldNames: immutableReadonlySet(schemaStringFieldNames(schema)),
    numericFieldNames: immutableReadonlySet(schemaNumericFieldNames(schema)),
    numberFieldNames: immutableReadonlySet(schemaNumberFieldNames(schema)),
    bigintFieldNames: immutableReadonlySet(schemaBigintFieldNames(schema)),
    bigDecimalFieldNames: immutableReadonlySet(schemaBigDecimalFieldNames(schema)),
    exactScalarEqualityFieldNames: immutableReadonlySet(
      schemaExactScalarEqualityFieldNames(schema),
    ),
    rangeValueKinds: immutableRangeValueKinds(schemaRangeValueKinds(schema)),
    valueSemantics: makeTopicRowValueSemantics(schema),
  });
  rawQueryCompilerMetadataSchemas.set(metadata, schema);
  return metadata;
};

export const rawQueryCompilerMetadataMatchesSchema = <
  SchemaValue extends Schema.Codec<object, unknown, never, never>,
>(
  metadata: RawQueryCompilerMetadata,
  schema: SchemaValue,
): metadata is RawQueryCompilerMetadata<SchemaValue["Type"]> =>
  metadata.schema === schema &&
  metadata[rawQueryCompilerMetadataSchema] === schema &&
  rawQueryCompilerMetadataSchemas.get(metadata) === schema &&
  topicRowValueSemanticsMatchesSchema(metadata.valueSemantics, schema);
