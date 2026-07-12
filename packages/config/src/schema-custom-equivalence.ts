import { SchemaAST } from "effect";
import { schemaAstChildren } from "./schema-ast-children";
import { viewSchemaDeclarationAstIsAdmitted } from "./view-schema";

const hasCustomEquivalence = (ast: SchemaAST.AST, seen: Set<SchemaAST.AST>): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  const equivalence = SchemaAST.resolve(ast)?.["toEquivalence"];
  if (
    equivalence !== undefined &&
    !(SchemaAST.isDeclaration(ast) && viewSchemaDeclarationAstIsAdmitted(ast))
  ) {
    return true;
  }
  return schemaAstChildren(ast).some((child) => hasCustomEquivalence(child, seen));
};

export const schemaHasCustomEquivalence = (ast: SchemaAST.AST): boolean =>
  hasCustomEquivalence(ast, new Set());
