import { Result, Schema, SchemaAST } from "effect";
import { viewSchemaDeclarationAstIsAdmitted } from "./view-schema";

type RouteFieldDomain = "empty" | "scalar" | "unsupported";

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isBigDecimalAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) &&
  viewSchemaDeclarationAstIsAdmitted(ast) &&
  isRecord(ast.annotations?.["typeConstructor"]) &&
  ast.annotations["typeConstructor"]["_tag"] === "effect/BigDecimal";

const routeFieldDomain = (ast: SchemaAST.AST, active: Set<SchemaAST.AST>): RouteFieldDomain => {
  if (SchemaAST.isUndefined(ast) || SchemaAST.isNever(ast)) {
    return "empty";
  }
  if (
    SchemaAST.isString(ast) ||
    SchemaAST.isTemplateLiteral(ast) ||
    SchemaAST.isNumber(ast) ||
    SchemaAST.isBigInt(ast) ||
    SchemaAST.isBoolean(ast) ||
    SchemaAST.isNull(ast) ||
    isBigDecimalAst(ast)
  ) {
    return "scalar";
  }
  if (SchemaAST.isLiteral(ast)) {
    return typeof ast.literal === "string" ||
      typeof ast.literal === "number" ||
      typeof ast.literal === "bigint" ||
      typeof ast.literal === "boolean"
      ? "scalar"
      : "unsupported";
  }
  if (SchemaAST.isEnum(ast)) {
    return ast.enums.length > 0 &&
      ast.enums.every(([, value]) => typeof value === "string" || typeof value === "number")
      ? "scalar"
      : "unsupported";
  }
  if (active.has(ast)) {
    return "unsupported";
  }
  active.add(ast);
  let domain: RouteFieldDomain;
  if (SchemaAST.isSuspend(ast)) {
    domain = routeFieldDomain(ast.thunk(), active);
  } else if (SchemaAST.isUnion(ast)) {
    domain = "empty";
    for (const member of ast.types) {
      const memberDomain = routeFieldDomain(member, active);
      if (memberDomain === "unsupported") {
        domain = "unsupported";
        break;
      }
      if (memberDomain === "scalar") {
        domain = "scalar";
      }
    }
  } else {
    domain = "unsupported";
  }
  active.delete(ast);
  return domain;
};

export const viewServerRouteFieldSchemaHasCompleteScalarDomain = (schema: unknown): boolean => {
  const result = Result.try(() =>
    Schema.isSchema(schema)
      ? routeFieldDomain(SchemaAST.toType(schema.ast), new Set()) === "scalar"
      : false,
  );
  return Result.isSuccess(result) && result.success;
};
