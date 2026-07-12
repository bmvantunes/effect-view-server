import { Schema, SchemaAST, SchemaGetter } from "effect";
import { schemaAstChildren } from "./schema-ast-children";
import { viewSchemaDeclarationAstIsAdmitted } from "./view-schema";

const canonicalEncodingSchemas = [
  Schema.BigIntFromString,
  Schema.BooleanFromBit,
  Schema.FiniteFromString,
  Schema.NumberFromString,
  Schema.StringFromUriComponent,
  Schema.Trim,
];

const canonicalEncodingTransformations = new Set(
  canonicalEncodingSchemas.map((schema) => schema.ast.encoding![0]!.transformation),
);

const isCanonicalDeclaration = (ast: SchemaAST.Declaration): boolean =>
  viewSchemaDeclarationAstIsAdmitted(ast);

const isCanonicalOptionalEncoding = (link: SchemaAST.Link): boolean =>
  SchemaAST.isOptional(link.to) &&
  Reflect.get(link.transformation, "encode") === SchemaGetter.passthrough();

const hasUnrecognizedCanonicalCodec = (ast: SchemaAST.AST, seen: Set<SchemaAST.AST>): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  if (SchemaAST.isDeclaration(ast) && !isCanonicalDeclaration(ast)) {
    return true;
  }
  if (
    ast.encoding?.some(
      (link) =>
        !(SchemaAST.isDeclaration(ast) && isCanonicalDeclaration(ast)) &&
        !isCanonicalOptionalEncoding(link) &&
        !canonicalEncodingTransformations.has(link.transformation),
    ) === true
  ) {
    return true;
  }
  return schemaAstChildren(ast).some((child) => hasUnrecognizedCanonicalCodec(child, seen));
};

export const schemaHasUnrecognizedCanonicalCodec = (ast: SchemaAST.AST): boolean =>
  hasUnrecognizedCanonicalCodec(ast, new Set());
