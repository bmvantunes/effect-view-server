import { SchemaAST } from "effect";

const schemaClassTypeId = Reflect.get(SchemaAST, "ClassTypeId");

export const schemaAstIsClass = (ast: SchemaAST.AST): boolean =>
  typeof schemaClassTypeId === "string" &&
  SchemaAST.isDeclaration(ast) &&
  ast.annotations?.[schemaClassTypeId] !== undefined;

export const schemaAstChildren = (ast: SchemaAST.AST): ReadonlyArray<SchemaAST.AST> => {
  const children: Array<SchemaAST.AST> = [];
  if (SchemaAST.isSuspend(ast)) {
    children.push(ast.thunk());
  }
  if (SchemaAST.isDeclaration(ast)) {
    children.push(...ast.typeParameters);
  }
  if (SchemaAST.isObjects(ast)) {
    for (const property of ast.propertySignatures) {
      children.push(property.type);
    }
    for (const index of ast.indexSignatures) {
      children.push(index.parameter, index.type);
    }
  }
  if (SchemaAST.isArrays(ast)) {
    children.push(...ast.elements, ...ast.rest);
  }
  if (SchemaAST.isUnion(ast)) {
    children.push(...ast.types);
  }
  if (ast.encoding !== undefined) {
    for (const link of ast.encoding) {
      children.push(link.to);
    }
  }
  return children;
};
