import { Schema, SchemaAST } from "effect";
import { schemaAstIsClass } from "./schema-ast-children";

const admittedDeclarationAsts = new WeakSet<SchemaAST.Declaration>([Schema.BigDecimal.ast]);

const registerDeclaration = <
  S extends Schema.Constraint & {
    readonly ast: SchemaAST.Declaration;
  },
>(
  schema: S,
): S => {
  admittedDeclarationAsts.add(schema.ast);
  return schema;
};

export const viewSchemaDeclarationAstIsAdmitted = (ast: SchemaAST.Declaration): boolean =>
  admittedDeclarationAsts.has(ast);

type ConcreteSchemaClass = Schema.Constraint & {
  readonly ast: SchemaAST.Declaration;
  readonly fields: Schema.Struct.Fields;
  readonly identifier: string;
};

const admitClass = <Class extends ConcreteSchemaClass>(schemaClass: Class): Class => {
  const ast = schemaClass.ast;
  if (!schemaAstIsClass(ast)) {
    throw new TypeError("viewSchema.admitClass requires a concrete Effect Schema.Class.");
  }
  admittedDeclarationAsts.add(ast);
  return schemaClass;
};

export const viewSchema = Object.freeze({
  BigDecimal: Schema.BigDecimal,
  Option: <A extends Schema.Constraint>(value: A): Schema.Option<A> =>
    registerDeclaration(Schema.Option(value)),
  Chunk: <Value extends Schema.Constraint>(value: Value): Schema.Chunk<Value> =>
    registerDeclaration(Schema.Chunk(value)),
  HashMap: <Key extends Schema.Constraint, Value extends Schema.Constraint>(
    key: Key,
    value: Value,
  ): Schema.HashMap<Key, Value> => registerDeclaration(Schema.HashMap(key, value)),
  HashSet: <Value extends Schema.Constraint>(value: Value): Schema.HashSet<Value> =>
    registerDeclaration(Schema.HashSet(value)),
  admitClass,
});
