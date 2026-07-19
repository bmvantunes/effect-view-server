import { Schema, SchemaAST } from "effect";
import { schemaAstIsClass } from "./schema-ast-children";

export type ViewServerFilterNumericKind = "number" | "bigint" | "bigDecimal";

export type ViewServerFilterFieldContract = {
  readonly path: string;
  readonly segments: ReadonlyArray<string>;
  readonly schema: Schema.Codec<unknown, unknown, never, never>;
  readonly typeSchema: Schema.Codec<unknown, unknown, never, never>;
  readonly numericKinds: ReadonlySet<ViewServerFilterNumericKind>;
  readonly supportsText: boolean;
};

type MutableFilterFieldContract = {
  readonly schemaAsts: Array<SchemaAST.AST>;
  readonly seenSchemaAsts: Set<SchemaAST.AST>;
  readonly seenTypeAsts: Set<SchemaAST.AST>;
  readonly typeAsts: Array<SchemaAST.AST>;
  readonly path: string;
  readonly segments: ReadonlyArray<string>;
  readonly numericKinds: Set<ViewServerFilterNumericKind>;
  supportsText: boolean;
};

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isBigDecimalAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) &&
  isRecord(ast.annotations?.["typeConstructor"]) &&
  ast.annotations["typeConstructor"]["_tag"] === "effect/BigDecimal";

const scalarMetadata = (
  ast: SchemaAST.AST,
):
  | {
      readonly numericKind?: ViewServerFilterNumericKind;
      readonly supportsText: boolean;
    }
  | undefined => {
  if (SchemaAST.isString(ast) || SchemaAST.isTemplateLiteral(ast)) {
    return { supportsText: true };
  }
  if (SchemaAST.isNumber(ast)) {
    return { supportsText: false, numericKind: "number" };
  }
  if (SchemaAST.isBigInt(ast)) {
    return { supportsText: false, numericKind: "bigint" };
  }
  if (isBigDecimalAst(ast)) {
    return { supportsText: false, numericKind: "bigDecimal" };
  }
  if (SchemaAST.isBoolean(ast) || SchemaAST.isNull(ast)) {
    return { supportsText: false };
  }
  if (SchemaAST.isLiteral(ast)) {
    return typeof ast.literal === "string"
      ? { supportsText: true }
      : typeof ast.literal === "number"
        ? { supportsText: false, numericKind: "number" }
        : typeof ast.literal === "bigint"
          ? { supportsText: false, numericKind: "bigint" }
          : { supportsText: false };
  }
  if (SchemaAST.isEnum(ast)) {
    return {
      supportsText: ast.enums.some(([, value]) => typeof value === "string"),
      ...(ast.enums.some(([, value]) => typeof value === "number")
        ? { numericKind: "number" as const }
        : {}),
    };
  }
  return undefined;
};

const addScalar = (
  fields: Map<string, MutableFilterFieldContract>,
  path: ReadonlyArray<string>,
  ast: SchemaAST.AST,
  metadata: Exclude<ReturnType<typeof scalarMetadata>, undefined>,
): void => {
  const name = path.join(".");
  let field = fields.get(name);
  if (field === undefined) {
    field = {
      schemaAsts: [],
      seenSchemaAsts: new Set(),
      seenTypeAsts: new Set(),
      typeAsts: [],
      path: name,
      segments: Object.freeze([...path]),
      numericKinds: new Set(),
      supportsText: false,
    };
    fields.set(name, field);
  }
  if (!field.seenSchemaAsts.has(ast)) {
    field.seenSchemaAsts.add(ast);
    field.schemaAsts.push(ast);
  }
  const decodedAst = SchemaAST.toType(ast);
  if (!field.seenTypeAsts.has(decodedAst)) {
    field.seenTypeAsts.add(decodedAst);
    field.typeAsts.push(decodedAst);
  }
  field.supportsText ||= metadata.supportsText;
  if (metadata.numericKind !== undefined) {
    field.numericKinds.add(metadata.numericKind);
  }
};

const collectFilterFields = (
  ast: SchemaAST.AST,
  path: ReadonlyArray<string>,
  fields: Map<string, MutableFilterFieldContract>,
  active: Set<SchemaAST.AST>,
): void => {
  const scalar = scalarMetadata(ast);
  if (scalar !== undefined) {
    addScalar(fields, path, ast, scalar);
    return;
  }
  if (active.has(ast)) {
    return;
  }
  active.add(ast);
  if (SchemaAST.isSuspend(ast)) {
    collectFilterFields(ast.thunk(), path, fields, active);
  } else if (SchemaAST.isUnion(ast)) {
    for (const member of ast.types) {
      collectFilterFields(member, path, fields, active);
    }
  } else if (SchemaAST.isObjects(ast)) {
    for (const property of ast.propertySignatures) {
      if (typeof property.name !== "string") {
        continue;
      }
      const propertyPath = [...path, property.name];
      if (property.name.includes(".")) {
        throw new TypeError(
          `Filterable object field ${propertyPath.join(".")} contains a reserved dot.`,
        );
      }
      collectFilterFields(property.type, propertyPath, fields, active);
    }
  } else if (SchemaAST.isDeclaration(ast) && schemaAstIsClass(ast)) {
    for (const parameter of ast.typeParameters) {
      collectFilterFields(parameter, path, fields, active);
    }
  }
  active.delete(ast);
};

const makeScalarSchema = (
  asts: ReadonlyArray<SchemaAST.AST>,
): Schema.Codec<unknown, unknown, never, never> =>
  Schema.make<Schema.Codec<unknown, unknown, never, never>>(
    asts.length === 1 ? asts[0]! : new SchemaAST.Union(asts, "anyOf"),
  );

type FilterRowSchema = object & {
  readonly fields?: Readonly<Record<string, unknown>>;
};

const cache = new WeakMap<object, ReadonlyMap<string, ViewServerFilterFieldContract>>();

export const viewServerFilterFieldContracts = (
  rowSchema: FilterRowSchema,
): ReadonlyMap<string, ViewServerFilterFieldContract> => {
  const cached = cache.get(rowSchema);
  if (cached !== undefined) {
    return cached;
  }
  const mutable = new Map<string, MutableFilterFieldContract>();
  for (const [field, fieldSchema] of Object.entries(rowSchema.fields ?? {})) {
    if (field.includes(".")) {
      throw new TypeError(`Filterable Topic Row field ${field} contains a reserved dot.`);
    }
    if (Schema.isSchema(fieldSchema)) {
      collectFilterFields(fieldSchema.ast, [field], mutable, new Set());
    }
  }
  const fields = new Map<string, ViewServerFilterFieldContract>();
  for (const field of mutable.values()) {
    fields.set(
      field.path,
      Object.freeze({
        path: field.path,
        segments: field.segments,
        schema: makeScalarSchema(field.schemaAsts),
        typeSchema: makeScalarSchema(field.typeAsts),
        numericKinds: Object.freeze(new Set(field.numericKinds)),
        supportsText: field.supportsText,
      }),
    );
  }
  cache.set(rowSchema, fields);
  return fields;
};

export const viewServerFilterFieldContract = (
  rowSchema: FilterRowSchema,
  path: string,
): ViewServerFilterFieldContract | undefined => viewServerFilterFieldContracts(rowSchema).get(path);
