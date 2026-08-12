import { SchemaAST } from "effect";

const schemaClassConstructorAnnotationKey = "~constructor";

const schemaClassConstructorDescriptorIsPresent = (ast: SchemaAST.Declaration): boolean => {
  try {
    const getDescriptor = Reflect.get(Object(ast.annotations), schemaClassConstructorAnnotationKey);
    if (typeof getDescriptor !== "function") {
      return false;
    }
    const descriptor = Reflect.apply(getDescriptor, undefined, [ast.typeParameters]);
    return (
      typeof descriptor === "object" &&
      descriptor !== null &&
      typeof Reflect.get(descriptor, "isConstructed") === "function" &&
      Reflect.get(descriptor, "link") !== undefined
    );
  } catch {
    return false;
  }
};

export const schemaAstIsClass = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) && schemaClassConstructorDescriptorIsPresent(ast);

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
