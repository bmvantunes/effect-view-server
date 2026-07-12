import { Array as Arr, SchemaAST } from "effect";
import { schemaAstChildren } from "./schema-ast-children";

const jsonNull = 1;
const jsonString = 2;
const jsonNumber = 4;
const jsonBoolean = 8;
const jsonArray = 16;
const jsonObject = 32;
const jsonAny = jsonNull | jsonString | jsonNumber | jsonBoolean | jsonArray | jsonObject;
const nullAstTags = new Set(["Null", "Undefined", "Void"]);
const stringAstTags = new Set(["String", "TemplateLiteral", "BigInt"]);

const literalToken = (value: SchemaAST.LiteralValue): string => JSON.stringify(value);

const literalShape = (value: SchemaAST.LiteralValue): number => {
  if (typeof value === "number") {
    return jsonNumber;
  }
  return typeof value === "boolean" ? jsonBoolean : jsonString;
};

const encodedShape = (ast: SchemaAST.AST, seen: Set<SchemaAST.AST> = new Set()): number => {
  if (seen.has(ast)) {
    return jsonAny;
  }
  if (SchemaAST.isSuspend(ast)) {
    seen.add(ast);
    return encodedShape(ast.thunk(), seen);
  }
  if (nullAstTags.has(ast._tag)) {
    return jsonNull;
  }
  if (stringAstTags.has(ast._tag)) {
    return jsonString;
  }
  if (SchemaAST.isNumber(ast)) {
    return jsonNumber;
  }
  if (SchemaAST.isBoolean(ast)) {
    return jsonBoolean;
  }
  if (SchemaAST.isArrays(ast)) {
    return jsonArray;
  }
  if (SchemaAST.isObjects(ast)) {
    return jsonObject;
  }
  return jsonAny;
};

const literalMayOverlap = (literal: SchemaAST.LiteralValue, ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isLiteral(ast)) {
    return literalToken(literal) === literalToken(ast.literal);
  }
  if (SchemaAST.isEnum(ast)) {
    return ast.enums.some(([, value]) => literalToken(literal) === literalToken(value));
  }
  return (literalShape(literal) & encodedShape(ast)) !== 0;
};

const fixedArrayElements = (ast: SchemaAST.Arrays): ReadonlyArray<SchemaAST.AST> | undefined =>
  ast.rest.length === 0 && !ast.elements.some(SchemaAST.isOptional) ? ast.elements : undefined;

const encodedArraysMayOverlap = (left: SchemaAST.Arrays, right: SchemaAST.Arrays): boolean => {
  const leftElements = fixedArrayElements(left);
  const rightElements = fixedArrayElements(right);
  if (leftElements === undefined || rightElements === undefined) {
    return true;
  }
  if (leftElements.length !== rightElements.length) {
    return false;
  }
  return Arr.zip(leftElements, rightElements).every(([leftElement, rightElement]) =>
    encodedAstsMayOverlap(leftElement, rightElement),
  );
};

const encodedObjectsMayOverlap = (left: SchemaAST.Objects, right: SchemaAST.Objects): boolean => {
  for (const leftProperty of left.propertySignatures) {
    if (SchemaAST.isOptional(leftProperty.type)) {
      continue;
    }
    const rightProperty = right.propertySignatures.find(
      (candidate) => candidate.name === leftProperty.name && !SchemaAST.isOptional(candidate.type),
    );
    if (
      rightProperty !== undefined &&
      !encodedAstsMayOverlap(leftProperty.type, rightProperty.type)
    ) {
      return false;
    }
  }
  return true;
};

function encodedAstsMayOverlap(leftInput: SchemaAST.AST, rightInput: SchemaAST.AST): boolean {
  const left = SchemaAST.toEncoded(leftInput);
  const right = SchemaAST.toEncoded(rightInput);
  if (SchemaAST.isUnion(left)) {
    return left.types.some((member) => encodedAstsMayOverlap(member, right));
  }
  if (SchemaAST.isUnion(right)) {
    return right.types.some((member) => encodedAstsMayOverlap(left, member));
  }
  if (SchemaAST.isLiteral(left)) {
    return literalMayOverlap(left.literal, right);
  }
  if (SchemaAST.isLiteral(right)) {
    return literalMayOverlap(right.literal, left);
  }
  if (SchemaAST.isEnum(left)) {
    return left.enums.some(([, value]) => literalMayOverlap(value, right));
  }
  if (SchemaAST.isEnum(right)) {
    return right.enums.some(([, value]) => literalMayOverlap(value, left));
  }
  if ((encodedShape(left) & encodedShape(right)) === 0) {
    return false;
  }
  if (SchemaAST.isArrays(left) && SchemaAST.isArrays(right)) {
    return encodedArraysMayOverlap(left, right);
  }
  if (SchemaAST.isObjects(left) && SchemaAST.isObjects(right)) {
    return encodedObjectsMayOverlap(left, right);
  }
  return true;
}

const unionHasOverlappingEncodings = (ast: SchemaAST.Union): boolean => {
  for (const [leftIndex, left] of ast.types.entries()) {
    for (const right of ast.types.slice(leftIndex + 1)) {
      if (encodedAstsMayOverlap(left, right)) {
        return true;
      }
    }
  }
  return false;
};

const hasAmbiguousJsonUnion = (ast: SchemaAST.AST, seen: Set<SchemaAST.AST>): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  return (
    (SchemaAST.isUnion(ast) && unionHasOverlappingEncodings(ast)) ||
    schemaAstChildren(ast).some((child) => hasAmbiguousJsonUnion(child, seen))
  );
};

export const schemaHasAmbiguousJsonUnion = (ast: SchemaAST.AST): boolean =>
  hasAmbiguousJsonUnion(ast, new Set());
